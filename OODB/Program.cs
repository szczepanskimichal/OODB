using System.Text.Json;
using Dapper;
using Microsoft.Data.Sqlite;

var builder = WebApplication.CreateBuilder(args);

var dbPath = Path.Combine(builder.Environment.ContentRootPath, "data", "app.db");
Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);
var connectionString = $"Data Source={dbPath};Cache=Shared;Mode=ReadWriteCreate;";

using (var connection = new SqliteConnection(connectionString))
{
    connection.Open();
    connection.Execute("""
        PRAGMA journal_mode = WAL;
        
        CREATE TABLE IF NOT EXISTS Events (
            EventId     TEXT PRIMARY KEY,
            OccurredUtc TEXT NOT NULL,
            RecordedUtc TEXT NOT NULL,
            Type        TEXT NOT NULL,
            
            -- Felles / nyttig for queries:
            StudentId   TEXT NULL,
            Course      TEXT NULL,
            Year        INTEGER NULL,
            Semester    INTEGER NULL,
            
            -- Student-info (kun for student_registrert):
            Name        TEXT NULL,
            Birthdate   TEXT NULL,
            City        TEXT NULL,
            
            -- Midtuke-evolusjon:
            Reason      TEXT NULL,
            
            -- Lagre original JSON uendret (debug/trace)
            PayloadJson TEXT NOT NULL
        );
       """);
    connection.Execute("""
        CREATE INDEX IF NOT EXISTS IX_Events_OccurredUtc ON Events(OccurredUtc);
        CREATE INDEX IF NOT EXISTS IX_Events_StudentId ON Events(StudentId);
        CREATE INDEX IF NOT EXISTS IX_Events_Course ON Events(Course, Year, Semester);
        """);

    void TryAdd(string sql)
    {
        try
        {
            connection.Execute(sql);
        }
        catch (SqliteException ex)
        {   
          /*  if(ex.SqliteErrorCode == 1 && ex.Message.Contains("duplicate column name"))
                return; // Kolumna już istnieje, można zignorować
            //Console.WriteLine(ex);
            throw;*/
          if (ex.SqliteErrorCode == 1)
          return;

          throw;
        }
    }
    TryAdd("ALTER TABLE Events ADD COLUMN StudentId TEXT;");
    TryAdd("ALTER TABLE Events ADD COLUMN Course TEXT;");
    TryAdd("ALTER TABLE Events ADD COLUMN Year INTEGER;");
    TryAdd("ALTER TABLE Events ADD COLUMN Semester INTEGER;");
    TryAdd("ALTER TABLE Events ADD COLUMN Name TEXT;");
    TryAdd("ALTER TABLE Events ADD COLUMN Birthdate TEXT;");
    TryAdd("ALTER TABLE Events ADD COLUMN City TEXT;");
    TryAdd("ALTER TABLE Events ADD COLUMN Reason TEXT;");
}

var app = builder.Build();
// ------------------------------------------------------------

static string? GetString(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

static int? GetInt(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i) ? i : null;

static bool Has(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out _);


// POST /events
// - append-only
// - idempotent: duplikat EventId -> { ok:false, error:"duplicate_event" }
// ------------------------------------------------------------
app.MapPost("/events", async (HttpRequest request) =>
{
    using var doc = await JsonDocument.ParseAsync(request.Body);
    var body = doc.RootElement;

    var eventId = GetString(body, "eventId");
    var occurredUtc = GetString(body, "occurredUtc");
    var recordedUtc = GetString(body, "recordedUtc");
    var type = GetString(body, "type");


    // Minimalna walidacja (na razie tylko to)
    if (string.IsNullOrWhiteSpace(eventId)) return Results.BadRequest(new { ok = false, error = "missing_eventId" });
    if (string.IsNullOrWhiteSpace(occurredUtc))
        return Results.BadRequest(new { ok = false, error = "missing_occurredUtc" });
    if (string.IsNullOrWhiteSpace(recordedUtc))
        return Results.BadRequest(new { ok = false, error = "missing_recordedUtc" });
    if (string.IsNullOrWhiteSpace(type)) return Results.BadRequest(new { ok = false, error = "missing_type" });
    //if (string.IsNullOrWhiteSpace(e.PayloadJson)) return Results.BadRequest(new { ok = false, error = "missing_payloadJson" });
    var payloadJson = body.GetRawText();
    using var connection = new SqliteConnection(connectionString);
    connection.Open();
/*
    // Idempotens: sjekk om eventet finnes
    var exists = await connection.ExecuteScalarAsync<long>(
        "SELECT COUNT(1) FROM Events WHERE EventId = @EventId;",
        new { e.EventId }
    );

    if (exists > 0)
        return Results.Ok(new { ok = false, error = "duplicate_event" });

    await connection.ExecuteAsync("""
        INSERT INTO Events (EventId, OccurredUtc, RecordedUtc, Type, PayloadJson)
        VALUES (@EventId, @OccurredUtc, @RecordedUtc, @Type, @PayloadJson);
    """, e);
*/
    var existing = await connection.QuerySingleOrDefaultAsync<(string Type, string? StudentId)?>(
        "SELECT Type, StudentId FROM Events WHERE EventId = @EventId;",
        new { eventId }
    );
    if (existing != null)
    {
        if (existing.Value.Type == "student_registrert")
            return Results.Ok(new { ok = false, error = "duplicate_event", studentId = existing.Value.StudentId });
        return Results.Ok(new { ok = false, error = "duplicate_event" });
    }

    if (type == "student_registrert")
    {
        if (Has(body, "studentId"))
            return Results.BadRequest(new { ok = false, error = "student_registrert_must_not_include_studentId" });
        var name = GetString(body, "name");
        var birthdate = GetString(body, "birthdate");
        var city = GetString(body, "city");
        if (string.IsNullOrWhiteSpace(name)) return Results.BadRequest(new { ok = false, error = "missing_name" });
        if (string.IsNullOrWhiteSpace(birthdate))
            return Results.BadRequest(new { ok = false, error = "missing_birthdate" });
        if (string.IsNullOrWhiteSpace(city)) return Results.BadRequest(new { ok = false, error = "missing_city" });

        var studentId = Guid.NewGuid().ToString();
        await connection.ExecuteAsync("""
                                          INSERT INTO Events (EventId, OccurredUtc, RecordedUtc, Type, StudentId, Name, Birthdate, City, PayloadJson)
                                          VALUES (@EventId, @OccurredUtc, @RecordedUtc, @Type, @StudentId, @Name, @Birthdate, @City, @PayloadJson);
                                      """, new
        {
            EventId = eventId,
            OccurredUtc = occurredUtc,
            RecordedUtc = recordedUtc,
            Type = type,
            StudentId = studentId,
            Name = name,
            Birthdate = birthdate,
            City = city,
            PayloadJson = payloadJson
        });
        return Results.Ok(new { ok = true, studentId });
    }
//-----------------------------------------

    var studentId2 = GetString(body, "studentId");
    var course = GetString(body, "course");
    var year = GetInt(body, "year");
    var semester = GetInt(body, "semester");
    var reason = GetString(body, "reason");

    if (string.IsNullOrWhiteSpace(studentId2))
        return Results.BadRequest(new { ok = false, error = "missing_studentId" });
    if (string.IsNullOrWhiteSpace(course)) return Results.BadRequest(new { ok = false, error = "missing_course" });
    if (year == null) return Results.BadRequest(new { ok = false, error = "missing_year" });
    if (semester == null) return Results.BadRequest(new { ok = false, error = "missing_semester" });

    await connection.ExecuteAsync("""
                                      INSERT INTO Events (
                                          EventId, 
                                          OccurredUtc, 
                                          RecordedUtc, 
                                          Type, 
                                          StudentId, 
                                          Course, 
                                          Year, 
                                          Semester, 
                                          Reason, 
                                          PayloadJson
                                          )
                                      VALUES (
                                              @EventId, 
                                              @OccurredUtc, 
                                              @RecordedUtc, 
                                              @Type, 
                                              @StudentId, 
                                              @Course, 
                                              @Year, 
                                              @Semester, 
                                              @Reason, 
                                              @PayloadJson);
                                  """, new
    {
        EventId = eventId,
        OccurredUtc = occurredUtc,
        RecordedUtc = recordedUtc,
        Type = type,
        StudentId = studentId2,
        Course = course,
        Year = year,
        Semester = semester,
        Reason = reason,
        PayloadJson = payloadJson
    });
    return Results.Ok(new { ok = true });
});

// ------------------------------------------------------------
// GET /events
// - lista eventów (do testów)
// ------------------------------------------------------------
app.MapGet("/events", async () =>
{
    using var connection = new SqliteConnection(connectionString);
    connection.Open();
    var rows = await connection.QueryAsync("""
                                               SELECT EventId, OccurredUtc, RecordedUtc, Type,
                                                      StudentId, Course, Year, Semester,
                                                      Name, Birthdate, City, Reason,
                                                      PayloadJson
                                               FROM Events
                                               ORDER BY OccurredUtc ASC, RecordedUtc ASC, EventId ASC;
                                           """);

    return Results.Ok(rows);
});

// ------------------------------------------------------------
// GET /debug/db
// - szybki podgląd: gdzie jest plik DB i ile jest eventów
// ------------------------------------------------------------
app.MapGet("/debug/db", async () =>
{
    using var connection = new SqliteConnection(connectionString);
    var count = await connection.ExecuteScalarAsync<long>("SELECT COUNT(1) FROM Events;");
    return Results.Ok(new { dbPath, eventsCount = count });
});

app.Run();

public sealed record RawEvent(
    string EventId,
    string OccurredUtc,
    string RecordedUtc,
    string Type,
    string PayloadJson
);
