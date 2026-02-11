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

app.Run();

public sealed record RawEvent(
    string EventId,
    string OccurredUtc,
    string RecordedUtc,
    string Type,
    string PayloadJson
);
