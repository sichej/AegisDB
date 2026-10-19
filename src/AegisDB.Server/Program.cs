
using AegisDB.Core;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;
using AegisDB.Core.Diagnostics;

var builder = WebApplication.CreateBuilder(args);

// Register the in-memory key-value store as a singleton.
builder.Services.AddSingleton<KeyValueStore>();

// Configure the WAL path.
// Integration tests can override it using Storage__WalPath.
string walPath = builder.Configuration["Storage:WalPath"]
    ?? Path.Combine(AppContext.BaseDirectory, "aegis.wal");

// Fault injection is disabled during normal execution.
// It can only be configured in the Testing environment.
IFaultInjector faultInjector = new NoOpFaultInjector();

if (builder.Environment.IsEnvironment("Testing"))
{
    string? faultDirectory =
        builder.Configuration["Testing:FaultDirectory"];

    if (!string.IsNullOrWhiteSpace(faultDirectory))
    {
        faultInjector = new FileFaultInjector(faultDirectory);
    }
}

// Register the WAL with the configured fault injector.
builder.Services.AddSingleton(
    new WriteAheadLog(walPath, faultInjector)
);

// Store snapshots alongside the WAL using an independent file.
string snapshotPath = builder.Configuration["Storage:SnapshotPath"]
    ?? Path.Combine(AppContext.BaseDirectory, "aegis.snapshot");

builder.Services.AddSingleton(new SnapshotStore(snapshotPath));

// Register the database engine.
builder.Services.AddSingleton<DatabaseEngine>();

// Register OpenAPI services.
builder.Services.AddOpenApi();

var app = builder.Build();

// Restore the latest validated snapshot and replay subsequent WAL records.
var recovery = new RecoveryManager(
    app.Services.GetRequiredService<WriteAheadLog>(),
    app.Services.GetRequiredService<KeyValueStore>(),
    app.Services.GetRequiredService<SnapshotStore>()
);

await recovery.RecoverAsync();

// Store or update a value.
app.MapPut("/kv/{key}", async (
    string key,
    HttpRequest request,
    DatabaseEngine db) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);

    await db.PutAsync(key, ms.ToArray());

    return Results.NoContent();
});

// Retrieve a value by its key.
app.MapGet("/kv/{key}", (string key, DatabaseEngine db) =>
{
    var value = db.Get(key);

    return value is null
        ? Results.NotFound()
        : Results.File(value, "application/octet-stream");
});

// Delete an existing key.
app.MapDelete("/kv/{key}", async (
    string key,
    DatabaseEngine db) =>
{
    bool deleted = await db.DeleteAsync(key);

    return deleted
        ? Results.NoContent()
        : Results.NotFound();
});

// Expose OpenAPI only during development.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

// Integration tests use HTTP on an ephemeral local port.
// HTTPS redirection remains enabled in other environments.
if (!app.Environment.IsEnvironment("Testing"))
{
    app.UseHttpsRedirection();
}

// Creates a persistent snapshot of the current database state.
// Intended for local development and testing.
app.MapPost("/admin/snapshot", async (
    DatabaseEngine db,
    SnapshotStore snapshotStore,
    CancellationToken cancellationToken) =>
{
    Snapshot snapshot = await db.CreateSnapshotAsync(cancellationToken);

    await snapshotStore.SaveAsync(snapshot, cancellationToken);

    return Results.Ok(new
    {
        walOffset = snapshot.WalOffset,
        entries = snapshot.Data.Count
    });
});

// Readiness is available only after WAL recovery completes.
app.MapGet("/health", () => Results.Ok("Healthy"));

app.Run();
