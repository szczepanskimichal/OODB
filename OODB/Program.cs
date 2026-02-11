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
            PayloadJson TEXT NOT NULL
        );
        
        CREATE INDEX IF NOT EXISTS IX_Events_OccurredUtc ON Events(OccurredUtc);
        """);
}

var app = builder.Build();
// ------------------------------------------------------------
// POST /events
// - append-only
// - idempotent: duplikat EventId -> { ok:false, error:"duplicate_event" }
// ------------------------------------------------------------
app.MapPost("/events", async (RawEvent e) =>
{
    // Minimalna walidacja (na razie tylko to)
    if (string.IsNullOrWhiteSpace(e.EventId)) return Results.BadRequest(new { ok = false, error = "missing_eventId" });
    if (string.IsNullOrWhiteSpace(e.OccurredUtc)) return Results.BadRequest(new { ok = false, error = "missing_occurredUtc" });
    if (string.IsNullOrWhiteSpace(e.RecordedUtc)) return Results.BadRequest(new { ok = false, error = "missing_recordedUtc" });
    if (string.IsNullOrWhiteSpace(e.Type)) return Results.BadRequest(new { ok = false, error = "missing_type" });
    if (string.IsNullOrWhiteSpace(e.PayloadJson)) return Results.BadRequest(new { ok = false, error = "missing_payloadJson" });

    using var connection = new SqliteConnection(connectionString);

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

    return Results.Ok(new { ok = true });
});

// ------------------------------------------------------------
// GET /events
// - lista eventów (do testów)
// ------------------------------------------------------------
app.MapGet("/events", async () =>
{
    using var connection = new SqliteConnection(connectionString);

    var rows = await connection.QueryAsync<RawEvent>("""
        SELECT EventId, OccurredUtc, RecordedUtc, Type, PayloadJson
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
