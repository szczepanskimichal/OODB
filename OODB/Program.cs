using System.Text.Json;
using Microsoft.Data.Sqlite;

var builder = WebApplication.CreateBuilder(args);

// DB-path: ./data/app.db
var dbPath = Path.Combine(builder.Environment.ContentRootPath, "data", "app.db");
Directory.CreateDirectory(Path.GetDirectoryName(dbPath)!);

var connectionString = $"Data Source={dbPath};Cache=Shared;Mode=ReadWriteCreate;";

// Init DB + mini-migrering
using (var connection = new SqliteConnection(connectionString))
{
    connection.Open();

    using (var cmd = connection.CreateCommand())
    {
        cmd.CommandText = """
            PRAGMA journal_mode = WAL;

            CREATE TABLE IF NOT EXISTS Events (
                EventId     TEXT PRIMARY KEY,
                OccurredUtc TEXT NOT NULL,
                RecordedUtc TEXT NOT NULL,
                Type        TEXT NOT NULL,
                PayloadJson TEXT NOT NULL
            );
        """;
        cmd.ExecuteNonQuery();
    }

    void TryAdd(string sql)
    {
        try
        {
            using (var cmd = connection.CreateCommand())
            {
                cmd.CommandText = sql;
                cmd.ExecuteNonQuery();
            }
        }
        catch (SqliteException ex)
        {
            if (ex.SqliteErrorCode == 1) return;
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

    using (var cmd = connection.CreateCommand())
    {
        cmd.CommandText = """
            CREATE INDEX IF NOT EXISTS IX_Events_OccurredUtc ON Events(OccurredUtc);
            CREATE INDEX IF NOT EXISTS IX_Events_StudentId ON Events(StudentId);
            CREATE INDEX IF NOT EXISTS IX_Events_Course ON Events(Course, Year, Semester);
        """;
        cmd.ExecuteNonQuery();
    }
}

var app = builder.Build();

// JSON helpers
static string? GetString(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.String ? v.GetString() : null;

static int? GetInt(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out var v) && v.ValueKind == JsonValueKind.Number && v.TryGetInt32(out var i) ? i : null;

static bool Has(JsonElement obj, string prop)
    => obj.TryGetProperty(prop, out _);

// POST /events
app.MapPost("/events", async (HttpRequest request) =>
{
    try
    {
        using var doc = await JsonDocument.ParseAsync(request.Body);
        var body = doc.RootElement;

        var eventId = GetString(body, "eventId");
        var occurredUtc = GetString(body, "occurredUtc");
        var recordedUtc = GetString(body, "recordedUtc");
        var type = GetString(body, "type");

        if (string.IsNullOrWhiteSpace(eventId)) return Results.BadRequest(new { ok = false, error = "missing_eventId" });
        if (string.IsNullOrWhiteSpace(occurredUtc)) return Results.BadRequest(new { ok = false, error = "missing_occurredUtc" });
        if (string.IsNullOrWhiteSpace(recordedUtc)) return Results.BadRequest(new { ok = false, error = "missing_recordedUtc" });
        if (string.IsNullOrWhiteSpace(type)) return Results.BadRequest(new { ok = false, error = "missing_type" });

        var payloadJson = body.GetRawText();

        using var connection = new SqliteConnection(connectionString);
        await connection.OpenAsync();

        using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT Type, StudentId FROM Events WHERE EventId = @eventId;";
        cmd.Parameters.AddWithValue("@eventId", eventId);
        
        using var reader = await cmd.ExecuteReaderAsync();
        EventRow? existing = null;
        if (await reader.ReadAsync())
        {
            existing = new EventRow
            {
                Type = reader.IsDBNull(0) ? null : reader.GetString(0),
                StudentId = reader.IsDBNull(1) ? null : reader.GetString(1)
            };
        }

        if (existing != null)
        {
            if (existing.Type == "student_registrert")
                return Results.Ok(new { ok = false, error = "duplicate_event", studentId = existing.StudentId });
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
            if (string.IsNullOrWhiteSpace(birthdate)) return Results.BadRequest(new { ok = false, error = "missing_birthdate" });
            if (string.IsNullOrWhiteSpace(city)) return Results.BadRequest(new { ok = false, error = "missing_city" });

            var studentId = Guid.NewGuid().ToString();

            using (var insertCmd = connection.CreateCommand())
            {
                insertCmd.CommandText = """
                    INSERT INTO Events (
                        EventId, OccurredUtc, RecordedUtc, Type,
                        StudentId, Name, Birthdate, City, PayloadJson
                    )
                    VALUES (
                        @EventId, @OccurredUtc, @RecordedUtc, @Type,
                        @StudentId, @Name, @Birthdate, @City, @PayloadJson
                    );
                """;
                insertCmd.Parameters.AddWithValue("@EventId", eventId);
                insertCmd.Parameters.AddWithValue("@OccurredUtc", occurredUtc);
                insertCmd.Parameters.AddWithValue("@RecordedUtc", recordedUtc);
                insertCmd.Parameters.AddWithValue("@Type", type);
                insertCmd.Parameters.AddWithValue("@StudentId", studentId);
                insertCmd.Parameters.AddWithValue("@Name", name);
                insertCmd.Parameters.AddWithValue("@Birthdate", birthdate);
                insertCmd.Parameters.AddWithValue("@City", city);
                insertCmd.Parameters.AddWithValue("@PayloadJson", payloadJson);

                await insertCmd.ExecuteNonQueryAsync();
            }

            return Results.Ok(new { ok = true, studentId });
        }

        var studentId2 = GetString(body, "studentId");
        var course = GetString(body, "course");
        var year = GetInt(body, "year");
        var semester = GetInt(body, "semester");
        var reason = GetString(body, "reason");

        if (string.IsNullOrWhiteSpace(studentId2)) return Results.BadRequest(new { ok = false, error = "missing_studentId" });
        if (string.IsNullOrWhiteSpace(course)) return Results.BadRequest(new { ok = false, error = "missing_course" });
        if (year == null) return Results.BadRequest(new { ok = false, error = "missing_year" });
        if (semester == null) return Results.BadRequest(new { ok = false, error = "missing_semester" });

        using (var insertCmd = connection.CreateCommand())
        {
            insertCmd.CommandText = """
                INSERT INTO Events (
                    EventId, OccurredUtc, RecordedUtc, Type,
                    StudentId, Course, Year, Semester, Reason,
                    PayloadJson
                )
                VALUES (
                    @EventId, @OccurredUtc, @RecordedUtc, @Type,
                    @StudentId, @Course, @Year, @Semester, @Reason,
                    @PayloadJson
                );
            """;
            insertCmd.Parameters.AddWithValue("@EventId", eventId);
            insertCmd.Parameters.AddWithValue("@OccurredUtc", occurredUtc);
            insertCmd.Parameters.AddWithValue("@RecordedUtc", recordedUtc);
            insertCmd.Parameters.AddWithValue("@Type", type);
            insertCmd.Parameters.AddWithValue("@StudentId", studentId2);
            insertCmd.Parameters.AddWithValue("@Course", course);
            insertCmd.Parameters.AddWithValue("@Year", year);
            insertCmd.Parameters.AddWithValue("@Semester", semester);
            insertCmd.Parameters.AddWithValue("@Reason", reason ?? (object)DBNull.Value);
            insertCmd.Parameters.AddWithValue("@PayloadJson", payloadJson);

            await insertCmd.ExecuteNonQueryAsync();
        }

        return Results.Ok(new { ok = true });
    }
    catch (Exception ex)
    {
        return Results.BadRequest(new { ok = false, error = "invalid_request", details = ex.Message });
    }
});

// GET /events
app.MapGet("/events", async () =>
{
    using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    var rows = new List<object>();
    using (var cmd = connection.CreateCommand())
    {
        cmd.CommandText = """
            SELECT EventId, OccurredUtc, RecordedUtc, Type,
                   StudentId, Course, Year, Semester,
                   Name, Birthdate, City, Reason,
                   PayloadJson
            FROM Events
            ORDER BY OccurredUtc ASC, RecordedUtc ASC, EventId ASC;
        """;

        using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var row = new
            {
                EventId = reader.GetString(0),
                OccurredUtc = reader.GetString(1),
                RecordedUtc = reader.GetString(2),
                Type = reader.GetString(3),
                StudentId = reader.IsDBNull(4) ? null : reader.GetString(4),
                Course = reader.IsDBNull(5) ? null : reader.GetString(5),
                Year = reader.IsDBNull(6) ? null : (int?)reader.GetInt32(6),
                Semester = reader.IsDBNull(7) ? null : (int?)reader.GetInt32(7),
                Name = reader.IsDBNull(8) ? null : reader.GetString(8),
                Birthdate = reader.IsDBNull(9) ? null : reader.GetString(9),
                City = reader.IsDBNull(10) ? null : reader.GetString(10),
                Reason = reader.IsDBNull(11) ? null : reader.GetString(11),
                PayloadJson = reader.GetString(12)
            };
            rows.Add(row);
        }
    }

    return Results.Ok(rows);
});

// GET /debug/db
app.MapGet("/debug/db", async () =>
{
    using var connection = new SqliteConnection(connectionString);
    await connection.OpenAsync();

    long count = 0;
    using (var cmd = connection.CreateCommand())
    {
        cmd.CommandText = "SELECT COUNT(1) FROM Events;";
        var result = await cmd.ExecuteScalarAsync();
        if (result != null)
            count = (long)result;
    }

    return Results.Ok(new { dbPath, eventsCount = count });
});

await app.RunAsync();

class EventRow
{
    public string? Type { get; set; }
    public string? StudentId { get; set; }
}
