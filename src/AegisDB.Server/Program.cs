using AegisDB.Core.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSingleton<KeyValueStore>();

// Add services to the container.
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();

var app = builder.Build();

app.MapPut("/kv/{key}", async (
    string key,
    HttpRequest request,
    KeyValueStore store) =>
{
    using var ms = new MemoryStream();
    await request.Body.CopyToAsync(ms);

    store.Put(key, ms.ToArray());

    return Results.NoContent();
});

app.MapGet("/kv/{key}", (string key, KeyValueStore store) =>
{
    var value = store.Get(key);

    if (value is null)
        return Results.NotFound();

    return Results.File(
        value,
        contentType: "application/octet-stream"
    );
});

app.MapDelete("/kv/{key}", (string key, KeyValueStore store) =>
{
    var deleted = store.Delete(key);

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

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    var forecast =  Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
})
.WithName("GetWeatherForecast");

app.Run();

record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
