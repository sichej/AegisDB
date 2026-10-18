using AegisDB.Core;
using AegisDB.Core.Persistence;
using AegisDB.Core.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<KeyValueStore>();

builder.Services.AddSingleton(
    new WriteAheadLog(
        Path.Combine(AppContext.BaseDirectory, "aegis.wal")
    )
);

builder.Services.AddSingleton<DatabaseEngine>();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

var recovery = new RecoveryManager(
    app.Services.GetRequiredService<WriteAheadLog>(),
    app.Services.GetRequiredService<KeyValueStore>()
);

// Restore persisted state before accepting HTTP requests.
await recovery.RecoverAsync();

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

app.MapGet("/kv/{key}", (string key, DatabaseEngine db) =>
{
    var value = db.Get(key);

    return value is null
        ? Results.NotFound()
        : Results.File(value, "application/octet-stream");
});

app.MapDelete("/kv/{key}", async (
    string key,
    DatabaseEngine db) =>
{
    bool deleted = await db.DeleteAsync(key);

    return deleted
        ? Results.NoContent()
        : Results.NotFound();
});

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();


app.Run();

