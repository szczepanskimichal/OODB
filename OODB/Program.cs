using System.Text.Json;
using Microsoft.Data.Sqlite;

// ==================== INICJALIZACJA APLIKACJI ====================
// Ta aplikacja zarządza zdarzeniami studenckich (events) w SQLite
// Obsługuje rejestracje studentów i ich statusy w kursach

var builder = WebApplication.CreateBuilder(args);

// ===== KONFIGURACJA BAZY DANYCH =====
// Baza danych SQLite będzie przechowywana w ./data/app.db
// Ścieżka jest względna do folderu projektu
var dbPath = Path.Combine(builder.Environment.ContentRootPath, "data", "app.db");
Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

// Connection string SQLite - umożliwia czytanie/pisanie i automatyczne tworzenie
// Cache=Shared - pozwala na wiele połączeń jednocześnie
// Mode=ReadWriteCreate - pozwala na odczyt, zapis i tworzenie bazy
var connectionString = $"Data Source={dbPath};Cache=Shared;Mode=ReadWriteCreate;";

// ===== INICJALIZACJA BAZY DANYCH I TWORZENIE TABEL =====
// Ta sekcja uruchamia się na starcie - tworzy tabele jeśli ich brak
using (var connection = new SqliteConnection(connectionString))
{
    connection.Open();

    // TABELA 1: Events - przechowuje wszystkie zdarzenia (append-only log)
    // Ta tabela jest "źródłem prawdy" - nigdy nie ma kasowania, tylko dodawanie
    // PRAGMA journal_mode = WAL - Write-Ahead Logging dla lepszej wydajności
    Exec(connection, """
        PRAGMA journal_mode = WAL;

        CREATE TABLE IF NOT EXISTS Events (
            -- Identyfikatory i czasomierze
            EventId     TEXT PRIMARY KEY,           -- Unikalny ID zdarzenia
            OccurredUtc TEXT NOT NULL,              -- Kiedy zdarzenie rzeczywiście się stało
            RecordedUtc TEXT NOT NULL,              -- Kiedy zdarzenie zostało zapisane w systemie
            Type        TEXT NOT NULL,              -- Typ zdarzenia (np. 'student_registrert', 'søkt', itd.)

            -- Informacje o studencie i kursie
            StudentId   TEXT NULL,                  -- ID studenta (NULL tylko dla rejestracji)
            Course      TEXT NULL,                  -- Nazwa kursu (np. 'Matematyka')
            Year        INTEGER NULL,               -- Rok akademicki (np. 2025)
            Semester    INTEGER NULL,               -- Semestr (1 lub 2)

            -- Dane osobowe studenta (tylko przy rejestracji)
            Name        TEXT NULL,                  -- Pełne imię i nazwisko
            Birthdate   TEXT NULL,                  -- Data urodzenia (ISO format)
            City        TEXT NULL,                  -- Miasto zamieszkania

            -- Inne info
            Reason      TEXT NULL,                  -- Powód zdarzenia (np. uzasadnienie)
            PayloadJson TEXT NOT NULL               -- Pełny JSON oryginału żądania
        );

        -- Indeksy dla szybszego wyszukiwania
        CREATE INDEX IF NOT EXISTS IX_Events_OccurredUtc ON Events(OccurredUtc);
        CREATE INDEX IF NOT EXISTS IX_Events_StudentId ON Events(StudentId);
        CREATE INDEX IF NOT EXISTS IX_Events_CourseRun ON Events(Course, Year, Semester);
    """);

    // TABELA 2: StatusByStudentCourseRun - przechowuje ostatni status każdego studenta w każdym kursie
    // To jest "pochodna" (derived state) zbudowana z tabeli Events
    // Można ją zawsze przebudować z tabel Events
    Exec(connection, """
        CREATE TABLE IF NOT EXISTS StatusByStudentCourseRun (
            -- Klucz - identyfikuje jednoznacznie kombinację student + kurs
            StudentId        TEXT NOT NULL,
            Course           TEXT NOT NULL,
            Year             INTEGER NOT NULL,
            Semester         INTEGER NOT NULL,

            -- Ostatni stan dla tej kombinacji
            LastType         TEXT NOT NULL,         -- Typ ostatniego zdarzenia
            LastOccurredUtc  TEXT NOT NULL,         -- Kiedy ostatnie zdarzenie się stało
            LastRecordedUtc  TEXT NOT NULL,         -- Kiedy ostatnie zdarzenie zostało zarejestrowane
            LastEventId      TEXT NOT NULL,         -- ID ostatniego zdarzenia
            Reason           TEXT NULL,             -- Powód ostatniego zdarzenia

            -- Klucz główny - unikatowa kombinacja studenta i kursu
            PRIMARY KEY (StudentId, Course, Year, Semester)
        );

        -- Indeksy dla szybszego wyszukiwania
        CREATE INDEX IF NOT EXISTS IX_Status_StudentId ON StatusByStudentCourseRun(StudentId);
        CREATE INDEX IF NOT EXISTS IX_Status_CourseRun ON StatusByStudentCourseRun(Course, Year, Semester);
    """);
}

var app = builder.Build();

// ===== POMOCNICZE FUNKCJE DO PARSOWANIA JSON =====
// Te funkcje bezpiecznie ekstrahują wartości z JSON, zwracając null jeśli brak lub zły typ
// Dzięki temu unikamy wyjątków i mamy czysty kod

// GetString - bezpiecznie pobiera wartość tekstową z JSON
// np. GetString(body, "name") -> "Jan Kowalski" lub null
static string? GetString(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

// GetInt - bezpiecznie pobiera wartość liczbową całkowitą z JSON
// np. GetInt(body, "year") -> 2025 lub null
static int? GetInt(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i) ? i : null;

// Has - sprawdza czy pole istnieje w JSON (niezależnie od wartości)
// np. Has(body, "studentId") -> true/false
static bool Has(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out _);

// ===== ENDPOINT: POST /events =====
// Główny endpoint do dodawania nowych zdarzeń
// Obsługuje dwa typy zdarzeń:
//   1. student_registrert - rejestracja nowego studenta (system generuje studentId)
//   2. Inne (søkt, itd.) - zdarzenia dla już zarejestrowanego studenta w konkretnym kursie
//
// Właściwości:
// - append-only: zdarzenia nigdy się nie kasują, tylko dodają
// - idempotentne: jeśli eventId już istnieje, zwracamy ok:false (nie duplikujemy)
// - stream-update: każde nowe zdarzenie automatycznie aktualizuje StatusByStudentCourseRun
app.MapPost("/events", async (HttpRequest request) =>
{
    // KROK 1: Parsuj JSON z żądania
    using var doc = await JsonDocument.ParseAsync(request.Body);
    var body = doc.RootElement;

    // KROK 2: Ekstrakhuj obowiązkowe pola wspólne dla wszystkich zdarzeń
    var eventId = GetString(body, "eventId");           // Unikalny identyfikator zdarzenia
    var occurredUtc = GetString(body, "occurredUtc");   // Kiedy się stało (według klienta)
    var recordedUtc = GetString(body, "recordedUtc");   // Kiedy nadesłano (timestamp serwera)
    var type = GetString(body, "type");                 // Typ: student_registrert, søkt, itd.

    // KROK 3: Waliduj obowiązkowe pola
    if (string.IsNullOrWhiteSpace(eventId)) return Results.BadRequest(new { ok = false, error = "missing_eventId" });
    if (string.IsNullOrWhiteSpace(occurredUtc)) return Results.BadRequest(new { ok = false, error = "missing_occurredUtc" });
    if (string.IsNullOrWhiteSpace(recordedUtc)) return Results.BadRequest(new { ok = false, error = "missing_recordedUtc" });
    if (string.IsNullOrWhiteSpace(type)) return Results.BadRequest(new { ok = false, error = "missing_type" });

    // Zachowaj oryginalny JSON dla auditowania
    var payloadJson = body.GetRawText();

    // KROK 4: Otwórz połączenie z bazą danych
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    // KROK 5: Sprawdź czy to zdarzenie już istnieje (idempotentność)
    // Jeśli istnieje, zwróć błąd ale z HTTP 200 (ok:false zamiast 400)
    var existing = await GetExistingEvent(connection, eventId);
    if (existing != null)
    {
        // Jeśli to była rejestracja, zwróć wygenerowany studentId
        if (existing.Value.Type == "student_registrert")
            return Results.Ok(new { ok = false, error = "duplicate_event", studentId = existing.Value.StudentId });

        return Results.Ok(new { ok = false, error = "duplicate_event" });
    }

    // ========== ŚCIEŻKA A: Rejestracja nowego studenta (student_registrert) ==========
    if (type == "student_registrert")
    {
        // Rejestracja NIE powinna zawierać studentId (system go wygeneruje)
        if (Has(body, "studentId"))
            return Results.BadRequest(new { ok = false, error = "student_registrert_must_not_include_studentId" });

        // Ekstrakhuj dane osobowe
        var name = GetString(body, "name");              // Pełne imię i nazwisko
        var birthdate = GetString(body, "birthdate");   // Data urodzenia
        var city = GetString(body, "city");             // Miasto zamieszkania

        // Waliduj dane osobowe
        if (string.IsNullOrWhiteSpace(name)) return Results.BadRequest(new { ok = false, error = "missing_name" });
        if (string.IsNullOrWhiteSpace(birthdate)) return Results.BadRequest(new { ok = false, error = "missing_birthdate" });
        if (string.IsNullOrWhiteSpace(city)) return Results.BadRequest(new { ok = false, error = "missing_city" });

        // WAŻNE: System generuje unikalny ID dla studenta
        var studentId = Guid.NewGuid().ToString();

        // Zapisz zdarzenie rejestracji
        await ExecAsync(connection, """
            INSERT INTO Events (
                EventId, OccurredUtc, RecordedUtc, Type,
                StudentId, Name, Birthdate, City,
                Course, Year, Semester, Reason,
                PayloadJson
            )
            VALUES (
                @EventId, @OccurredUtc, @RecordedUtc, @Type,
                @StudentId, @Name, @Birthdate, @City,
                NULL, NULL, NULL, NULL,
                @PayloadJson
            );
        """, new Dictionary<string, object?> {
            ["@EventId"] = eventId,
            ["@OccurredUtc"] = occurredUtc,
            ["@RecordedUtc"] = recordedUtc,
            ["@Type"] = type,
            ["@StudentId"] = studentId,
            ["@Name"] = name,
            ["@Birthdate"] = birthdate,
            ["@City"] = city,
            ["@PayloadJson"] = payloadJson
        });

        // Zwróć nowo wygenerowany studentId klientowi
        return Results.Ok(new { ok = true, studentId });
    }

    // ========== ŚCIEŻKA B: Pozostałe zdarzenia (søkt, itd.) ==========
    // Te zdarzenia dotyczą konkretnego studenta w konkretnym kursie
    
    // Ekstrakhuj dane ze zdarzenia
    var studentId2 = GetString(body, "studentId");      // Student musi już istnieć
    var course = GetString(body, "course");             // Nazwa kursu
    var year = GetInt(body, "year");                    // Rok akademicki
    var semester = GetInt(body, "semester");            // Semestr (1 lub 2)
    var reason = GetString(body, "reason");             // Opcjonalnie: powód (np. uzasadnienie)

    // Waliduj obowiązkowe pola
    if (string.IsNullOrWhiteSpace(studentId2)) return Results.BadRequest(new { ok = false, error = "missing_studentId" });
    if (string.IsNullOrWhiteSpace(course)) return Results.BadRequest(new { ok = false, error = "missing_course" });
    if (year == null) return Results.BadRequest(new { ok = false, error = "missing_year" });
    if (semester == null) return Results.BadRequest(new { ok = false, error = "missing_semester" });

    // Zapisz zdarzenie w tabeli Events
    await ExecAsync(connection, """
        INSERT INTO Events (
            EventId, OccurredUtc, RecordedUtc, Type,
            StudentId, Course, Year, Semester,
            Reason,
            PayloadJson
        )
        VALUES (
            @EventId, @OccurredUtc, @RecordedUtc, @Type,
            @StudentId, @Course, @Year, @Semester,
            @Reason,
            @PayloadJson
        );
    """, new Dictionary<string, object?> {
        ["@EventId"] = eventId,
        ["@OccurredUtc"] = occurredUtc,
        ["@RecordedUtc"] = recordedUtc,
        ["@Type"] = type,
        ["@StudentId"] = studentId2,
        ["@Course"] = course,
        ["@Year"] = year.Value,
        ["@Semester"] = semester.Value,
        ["@Reason"] = reason,
        ["@PayloadJson"] = payloadJson
    });

    // WAŻNE: Automatycznie aktualizuj tabeli StatusByStudentCourseRun
    // Ta tabela przechowuje OSTATNI status każdego studenta w każdym kursie
    await UpsertStatusIfNewer(connection,
        studentId2!, course!, year.Value, semester.Value,
        type!, occurredUtc!, recordedUtc!, eventId!, reason);

    return Results.Ok(new { ok = true });
});

// ===== ENDPOINT: GET /events (debug) =====
// Zwraca wszystkie zdarzenia z bazy danych
// Sortuje po OccurredUtc -> RecordedUtc -> EventId
// Przydatne do debugowania i audytu
app.MapGet("/events", async () =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var rows = new List<object>();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = """
        SELECT EventId, OccurredUtc, RecordedUtc, Type,
               StudentId, Course, Year, Semester,
               Name, Birthdate, City, Reason,
               PayloadJson
        FROM Events
        ORDER BY OccurredUtc ASC, RecordedUtc ASC, EventId ASC;
    """;

    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        rows.Add(new
        {
            EventId = r.GetString(0),
            OccurredUtc = r.GetString(1),
            RecordedUtc = r.GetString(2),
            Type = r.GetString(3),
            StudentId = r.IsDBNull(4) ? null : r.GetString(4),
            Course = r.IsDBNull(5) ? null : r.GetString(5),
            Year = r.IsDBNull(6) ? null : (int?)r.GetInt32(6),
            Semester = r.IsDBNull(7) ? null : (int?)r.GetInt32(7),
            Name = r.IsDBNull(8) ? null : r.GetString(8),
            Birthdate = r.IsDBNull(9) ? null : r.GetString(9),
            City = r.IsDBNull(10) ? null : r.GetString(10),
            Reason = r.IsDBNull(11) ? null : r.GetString(11),
            PayloadJson = r.GetString(12)
        });
    }

    return Results.Ok(rows);
});

// ===== ENDPOINT: GET /status =====
// Zwraca bieżący status studentów w kursach (derived state)
// Opcjonalnie filtruje po studentId (GET /status?studentId=...)
// Ta tabela zawiera OSTATNI status dla każdej kombinacji (student, course, year, semester)
app.MapGet("/status", async (string? studentId) =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var list = new List<object>();

    await using var cmd = connection.CreateCommand();
    if (!string.IsNullOrWhiteSpace(studentId))
    {
        cmd.CommandText = """
            SELECT StudentId, Course, Year, Semester,
                   LastType, LastOccurredUtc, LastRecordedUtc, LastEventId, Reason
            FROM StatusByStudentCourseRun
            WHERE StudentId = @StudentId
            ORDER BY Year ASC, Semester ASC, Course ASC;
        """;
        cmd.Parameters.AddWithValue("@StudentId", studentId);
    }
    else
    {
        cmd.CommandText = """
            SELECT StudentId, Course, Year, Semester,
                   LastType, LastOccurredUtc, LastRecordedUtc, LastEventId, Reason
            FROM StatusByStudentCourseRun
            ORDER BY Year ASC, Semester ASC, Course ASC, StudentId ASC;
        """;
    }

    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        list.Add(new
        {
            studentId = r.GetString(0),
            course = r.GetString(1),
            year = r.GetInt32(2),
            semester = r.GetInt32(3),
            lastType = r.GetString(4),
            lastOccurredUtc = r.GetString(5),
            lastRecordedUtc = r.GetString(6),
            lastEventId = r.GetString(7),
            reason = r.IsDBNull(8) ? null : r.GetString(8)
        });
    }

    return Results.Ok(list);
});

// ===== ENDPOINT: GET /stats =====
// Zwraca statystyki: ile studentów w każdym statusie na każdym kursie
// Grupuje po (course, year, semester, lastType) i liczy liczbę studentów
app.MapGet("/stats", async () =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var result = new List<object>();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = """
        SELECT Course, Year, Semester, LastType, COUNT(1) AS Cnt
        FROM StatusByStudentCourseRun
        GROUP BY Course, Year, Semester, LastType
        ORDER BY Year ASC, Semester ASC, Course ASC, LastType ASC;
    """;

    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        result.Add(new
        {
            course = r.GetString(0),
            year = r.GetInt32(1),
            semester = r.GetInt32(2),
            status = r.GetString(3),
            count = r.GetInt64(4)
        });
    }

    return Results.Ok(result);
});

// ===== ENDPOINT: GET /stats/total =====
// Zwraca statystyki zagregowane po WSZYSTKIE LATA
// Grupuje po (course, semester, lastType) - bez Years
// Przydatne do: "Ile studentów ukończyło Frontend niezależnie od roku?"
app.MapGet("/stats/total", async () =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var result = new List<object>();

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = """
        SELECT Course, Semester, LastType, COUNT(1) AS Cnt
        FROM StatusByStudentCourseRun
        GROUP BY Course, Semester, LastType
        ORDER BY Course ASC, Semester ASC, LastType ASC;
    """;

    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        result.Add(new
        {
            course = r.GetString(0),
            semester = r.GetInt32(1),
            status = r.GetString(2),
            count = r.GetInt64(3)
        });
    }

    return Results.Ok(result);
});

// ===== ENDPOINT: POST /batch/rebuild =====
// Przebudowuje derived state (tabelę StatusByStudentCourseRun) z całej historii Events
// Przydatne jeśli:
// - Coś poszło nie tak z StatusByStudentCourseRun
// - Chcesz przeliczyć wszystkie statusy od nowa
// - Zmieniła się logika obliczania statusów
app.MapPost("/batch/rebuild", async () =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    await ExecAsync(connection, "DELETE FROM StatusByStudentCourseRun;", null);

    await using var cmd = connection.CreateCommand();
    cmd.CommandText = """
        SELECT EventId, OccurredUtc, RecordedUtc, Type,
               StudentId, Course, Year, Semester, Reason
        FROM Events
        WHERE Type != 'student_registrert'
          AND StudentId IS NOT NULL
          AND Course IS NOT NULL
          AND Year IS NOT NULL
          AND Semester IS NOT NULL
        ORDER BY OccurredUtc ASC, RecordedUtc ASC, EventId ASC;
    """;

    await using var r = await cmd.ExecuteReaderAsync();
    while (await r.ReadAsync())
    {
        var eventId = r.GetString(0);
        var occurredUtc = r.GetString(1);
        var recordedUtc = r.GetString(2);
        var type = r.GetString(3);

        var studentId = r.GetString(4);
        var course = r.GetString(5);
        var year = r.GetInt32(6);
        var semester = r.GetInt32(7);
        var reason = r.IsDBNull(8) ? null : r.GetString(8);

        await UpsertStatusIfNewer(connection, studentId, course, year, semester, type, occurredUtc, recordedUtc, eventId, reason);
    }

    return Results.Ok(new { ok = true });
});

// ===== ENDPOINT: GET /debug/db =====
// Zwraca informacje debugowania o bazie danych
// - dbPath: ścieżka do pliku bazy danych
// - eventsCount: liczba zdarzeń w tabeli Events
app.MapGet("/debug/db", async () =>
{
    await using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    long count;
    await using (var cmd = connection.CreateCommand())
    {
        cmd.CommandText = "SELECT COUNT(1) FROM Events;";
        count = (long)(await cmd.ExecuteScalarAsync() ?? 0L);
    }

    return Results.Ok(new { dbPath, eventsCount = count });
});

app.Run();


// ======================================================================
// POMOCNICZE FUNKCJE DO OBSŁUGI BAZY DANYCH
// ======================================================================

// Exec - synchroniczna wersja wykonywania SQL bez parametrów
// Używana do inicjalizacji bazy danych (PRAGMA, CREATE TABLE, CREATE INDEX)
static void Exec(SqliteConnection connection, string sql)
{
    using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;
    cmd.ExecuteNonQuery();
}

// ExecAsync - asynchroniczna wersja wykonywania SQL z parametrami
// Używana do INSERT, UPDATE, DELETE w endpointach
static async Task ExecAsync(SqliteConnection connection, string sql, Dictionary<string, object?>? p)
{
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = sql;

    // Dodaj parametry do komendy (chroni przed SQL injection)
    if (p != null)
    {
        foreach (var kv in p)
            cmd.Parameters.AddWithValue(kv.Key, kv.Value ?? DBNull.Value);
    }

    await cmd.ExecuteNonQueryAsync();
}

// GetExistingEvent - sprawdza czy dany eventId już istnieje w bazie
// Zwraca tuple (Type, StudentId) jeśli istnieje, lub null jeśli nie
static async Task<(string Type, string? StudentId)?> GetExistingEvent(SqliteConnection connection, string eventId)
{
    await using var cmd = connection.CreateCommand();
    cmd.CommandText = "SELECT Type, StudentId FROM Events WHERE EventId = @EventId;";
    cmd.Parameters.AddWithValue("@EventId", eventId);

    await using var r = await cmd.ExecuteReaderAsync();
    if (!await r.ReadAsync()) return null;

    var type = r.GetString(0);
    var studentId = r.IsDBNull(1) ? null : r.GetString(1);
    return (type, studentId);
}

// UpsertStatusIfNewer - upsert (insert or update) do StatusByStudentCourseRun
// Ale TYLKO jeśli nowe zdarzenie jest "nowsze" niż ostatnie znane zdarzenie
// Porównuje: pierwszy OccurredUtc (leksykograficznie), potem RecordedUtc
// To gwarantuje, że historyczny status nie będzie nadpisany nowszym czasowo zdarzeniem
// (nawet jeśli zdarzenia dochodziły out-of-order)
static async Task UpsertStatusIfNewer(
    SqliteConnection connection,
    string studentId,
    string course,
    int year,
    int semester,
    string type,
    string occurredUtc,
    string recordedUtc,
    string eventId,
    string? reason)
{
    // KROK 1: Sprawdź czy ta kombinacja (student, course, year, semester) już istnieje
    await using var check = connection.CreateCommand();
    check.CommandText = """
        SELECT LastOccurredUtc, LastRecordedUtc
        FROM StatusByStudentCourseRun
        WHERE StudentId=@StudentId AND Course=@Course AND Year=@Year AND Semester=@Semester;
    """;
    check.Parameters.AddWithValue("@StudentId", studentId);
    check.Parameters.AddWithValue("@Course", course);
    check.Parameters.AddWithValue("@Year", year);
    check.Parameters.AddWithValue("@Semester", semester);

    await using var r = await check.ExecuteReaderAsync();
    if (await r.ReadAsync())
    {
        // KROK 2: Jeśli istnieje, sprawdź czy nowe zdarzenie jest "nowsze"
        var lastOccurred = r.GetString(0);
        var lastRecorded = r.GetString(1);

        // Porównanie: najpierw OccurredUtc (ISO string comparison), potem RecordedUtc
        // Dzięki ISO 8601 format (YYYY-MM-DDTHH:MM:SSZ), string comparison działa poprawnie
        var isNewer =
            string.Compare(occurredUtc, lastOccurred, StringComparison.Ordinal) > 0 ||
            (string.Compare(occurredUtc, lastOccurred, StringComparison.Ordinal) == 0 &&
             string.Compare(recordedUtc, lastRecorded, StringComparison.Ordinal) > 0);

        // Jeśli nowe zdarzenie NIE jest nowsze, zignoruj je (maintain last known state)
        if (!isNewer) return;

        // KROK 3: Jeśli jest nowsze, zaktualizuj
        await ExecAsync(connection, """
            UPDATE StatusByStudentCourseRun
            SET LastType=@LastType,
                LastOccurredUtc=@LastOccurredUtc,
                LastRecordedUtc=@LastRecordedUtc,
                LastEventId=@LastEventId,
                Reason=@Reason
            WHERE StudentId=@StudentId AND Course=@Course AND Year=@Year AND Semester=@Semester;
        """, new Dictionary<string, object?> {
            ["@LastType"] = type,
            ["@LastOccurredUtc"] = occurredUtc,
            ["@LastRecordedUtc"] = recordedUtc,
            ["@LastEventId"] = eventId,
            ["@Reason"] = reason,
            ["@StudentId"] = studentId,
            ["@Course"] = course,
            ["@Year"] = year,
            ["@Semester"] = semester
        });

        return;
    }

    // KROK 4: Jeśli NIE istnieje, wstaw nowy wiersz
    await ExecAsync(connection, """
        INSERT INTO StatusByStudentCourseRun (
            StudentId, Course, Year, Semester,
            LastType, LastOccurredUtc, LastRecordedUtc, LastEventId, Reason
        )
        VALUES (
            @StudentId, @Course, @Year, @Semester,
            @LastType, @LastOccurredUtc, @LastRecordedUtc, @LastEventId, @Reason
        );
    """, new Dictionary<string, object?> {
        ["@StudentId"] = studentId,
        ["@Course"] = course,
        ["@Year"] = year,
        ["@Semester"] = semester,
        ["@LastType"] = type,
        ["@LastOccurredUtc"] = occurredUtc,
        ["@LastRecordedUtc"] = recordedUtc,
        ["@LastEventId"] = eventId,
        ["@Reason"] = reason
    });
}
