using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AegisDB.Tests;

/// <summary>
/// Integration tests that verify AegisDB can start independently and recover
/// confirmed writes after its server process is terminated unexpectedly.
/// </summary>
public sealed class CrashRecoveryTests
{
    /// <summary>
    /// Resolves the compiled server assembly for the current test configuration.
    /// Build AegisDB.Server before running these tests.
    /// </summary>
    private static string GetServerDllPath()
    {
#if DEBUG
        const string configuration = "Debug";
#else
        const string configuration = "Release";
#endif
        string path = Path.GetFullPath(Path.Combine(
            AppContext.BaseDirectory,
            "../../../../../src/AegisDB.Server/bin",
            configuration,
            "net10.0",
            "AegisDB.Server.dll"));

        Assert.True(File.Exists(path),
            $"Server assembly not found: {path}. Build AegisDB.Server first.");

        return path;
    }

    /// <summary>
    /// Starts a separate server process using a test-specific WAL file.
    /// Both output streams are drained to avoid blocking on full pipe buffers.
    /// </summary>
    private static Process StartServer(
    string baseUrl,
    string walPath,
    string? faultDirectory = null)
    {
        string serverDllPath = GetServerDllPath();

        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            WorkingDirectory = Path.GetDirectoryName(serverDllPath)!,
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        startInfo.ArgumentList.Add(serverDllPath);
        startInfo.ArgumentList.Add("--urls");
        startInfo.ArgumentList.Add(baseUrl);

        // Reuse the same WAL path on restart, but isolate separate tests.
        startInfo.Environment["Storage__WalPath"] = walPath;
        startInfo.Environment["Storage__SnapshotPath"] = Path.Combine(Path.GetDirectoryName(walPath)!, "test.snapshot");
        startInfo.Environment["ASPNETCORE_ENVIRONMENT"] = "Testing";

        // Enable cross-process fault injection only when requested.
        if (faultDirectory is not null)
        {
            startInfo.Environment["Testing__FaultDirectory"] = faultDirectory;
        }

        var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException("Failed to start AegisDB.Server.");

        CaptureProcessOutput(process);
        return process;
    }

    /// <summary>
    /// Forcibly terminates a server process and releases its resources.
    /// </summary>
    private static async Task StopServerAsync(Process process)
    {
        try
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);

            await process.WaitForExitAsync();
        }
        finally
        {
            process.Dispose();
        }
    }

    /// <summary>
    /// Selects a currently available TCP port. There is a small race between
    /// releasing this port and the server binding to it.
    /// </summary>
    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        try
        {
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            listener.Stop();
        }
    }

    /// <summary>
    /// Waits until the server answers its health endpoint, or fails if the
    /// process exits or the startup deadline is exceeded.
    /// </summary>
    private static async Task WaitForServerReadyAsync(
        Process process,
        string baseUrl,
        CancellationToken cancellationToken = default)
    {
        using var client = new HttpClient { Timeout = TimeSpan.FromSeconds(1) };
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();

            if (process.HasExited)
                throw new InvalidOperationException(
                    $"Server exited with code {process.ExitCode}.");

            try
            {
                using var response = await client.GetAsync(
                    $"{baseUrl}/health", timeout.Token);

                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // The server may not be listening yet.
            }
            catch (TaskCanceledException) when (!timeout.IsCancellationRequested)
            {
                // An individual health check can time out during startup.
            }

            await Task.Delay(200, timeout.Token);
        }
    }

    /// <summary>
    /// Drains stdout and stderr while the server is running.
    /// </summary>
    private static void CaptureProcessOutput(Process process)
    {
        process.OutputDataReceived += (_, args) =>
        {
            if (args.Data is not null)
                Console.WriteLine($"[SERVER] {args.Data}");
        };

        process.ErrorDataReceived += (_, args) =>
        {
            if (args.Data is not null)
                Console.Error.WriteLine($"[SERVER ERROR] {args.Data}");
        };

        process.BeginOutputReadLine();
        process.BeginErrorReadLine();
    }

    /// <summary>
    /// Creates a unique WAL directory that is safe to delete after the server exits.
    /// </summary>
    private static string CreateTestDirectory()
    {
        string path = Path.Combine(
            Path.GetTempPath(), $"aegis-crash-{Guid.NewGuid():N}");
        Directory.CreateDirectory(path);
        return path;
    }

    [Fact]
    public async Task Server_ShouldStartAsSeparateProcess()
    {
        string testDirectory = CreateTestDirectory();
        string walPath = Path.Combine(testDirectory, "test.wal");
        string baseUrl = $"http://127.0.0.1:{GetAvailablePort()}";
        Process? server = null;

        try
        {
            server = StartServer(baseUrl, walPath);
            await WaitForServerReadyAsync(server, baseUrl);
            Assert.False(server.HasExited);
        }
        finally
        {
            if (server is not null)
                await StopServerAsync(server);

            Directory.Delete(testDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Server_ShouldRecoverDataAfterProcessCrash()
    {
        string testDirectory = CreateTestDirectory();
        string walPath = Path.Combine(testDirectory, "test.wal");
        string baseUrl = $"http://127.0.0.1:{GetAvailablePort()}";
        Process? server = null;

        try
        {
            using var client = new HttpClient { BaseAddress = new Uri(baseUrl) };

            server = StartServer(baseUrl, walPath);
            await WaitForServerReadyAsync(server, baseUrl);

            using (var response = await client.PutAsync(
                "/kv/user:1", new ByteArrayContent(Encoding.UTF8.GetBytes("Alice"))))
            {
                Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
            }

            // Kill the server instead of requesting a graceful shutdown.
            await StopServerAsync(server);
            server = null;

            server = StartServer(baseUrl, walPath);
            await WaitForServerReadyAsync(server, baseUrl);

            using var responseAfterRestart = await client.GetAsync("/kv/user:1");
            Assert.Equal(HttpStatusCode.OK, responseAfterRestart.StatusCode);
            Assert.Equal("Alice", await responseAfterRestart.Content.ReadAsStringAsync());
        }
        finally
        {
            if (server is not null)
                await StopServerAsync(server);

            Directory.Delete(testDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Server_ShouldPreserveConfirmedWritesAfterConcurrentCrash()
    {
        const int numberOfWrites = 200;

        string testDirectory = CreateTestDirectory();
        string walPath = Path.Combine(testDirectory, "test.wal");
        string baseUrl = $"http://127.0.0.1:{GetAvailablePort()}";
        Process? server = null;

        try
        {
            server = StartServer(baseUrl, walPath);
            await WaitForServerReadyAsync(server, baseUrl);

            using var client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl),
                Timeout = TimeSpan.FromSeconds(10)
            };

            var confirmedWrites = new ConcurrentDictionary<string, string>();

            // Each task uses its own key, so confirmation can be checked independently.
            Task[] tasks = Enumerable.Range(0, numberOfWrites)
                .Select(async i =>
                {
                    string key = $"user:{i}";
                    string value = $"value:{i}";

                    try
                    {
                        using var response = await client.PutAsync(
                            $"/kv/{key}",
                            new ByteArrayContent(Encoding.UTF8.GetBytes(value)));

                        if (response.StatusCode == HttpStatusCode.NoContent)
                        {
                            confirmedWrites.TryAdd(key, value);
                        }
                        else
                        {
                            throw new InvalidOperationException(
                                $"Unexpected PUT status for {key}: {response.StatusCode}");
                        }
                    }
                    catch (HttpRequestException)
                    {
                        // Termination may drop requests whose outcomes are unknown.
                    }
                    catch (TaskCanceledException)
                    {
                        // A request may time out before its outcome is known.
                    }
                })
                .ToArray();

            // Wait until at least one write has been confirmed before crashing.
            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(10)
            );

            while (confirmedWrites.IsEmpty)
            {
                timeout.Token.ThrowIfCancellationRequested();

                // Avoid busy-waiting while requests are being processed.
                await Task.Delay(10, timeout.Token);
            }

            await StopServerAsync(server);
            server = null;

            await Task.WhenAll(tasks);
            Assert.NotEmpty(confirmedWrites);

            server = StartServer(baseUrl, walPath);
            await WaitForServerReadyAsync(server, baseUrl);

            foreach (var (key, expectedValue) in confirmedWrites)
            {
                using var response = await client.GetAsync($"/kv/{key}");
                Assert.Equal(HttpStatusCode.OK, response.StatusCode);
                Assert.Equal(expectedValue, await response.Content.ReadAsStringAsync());
            }
        }
        finally
        {
            if (server is not null)
                await StopServerAsync(server);

            Directory.Delete(testDirectory, recursive: true);
        }
    }

    [Fact]
    public async Task Server_ShouldRecoverAfterCrashBeforeWalFlush()
    {
        string testDirectory = Path.Combine(
            Path.GetTempPath(),
            $"aegis-fault-{Guid.NewGuid():N}"
        );

        string faultDirectory = Path.Combine(testDirectory, "faults");
        string walPath = Path.Combine(testDirectory, "test.wal");
        string baseUrl = $"http://127.0.0.1:{GetAvailablePort()}";

        Directory.CreateDirectory(faultDirectory);

        string checkpoint = "wal.before_flush";

        string enabledPath = Path.Combine(
            faultDirectory, $"{checkpoint}.enabled");

        string reachedPath = Path.Combine(
            faultDirectory, $"{checkpoint}.reached");

        // Activate the checkpoint before starting the server.
        await File.WriteAllTextAsync(enabledPath, "");

        Process? server = null;

        try
        {
            server = StartServer(baseUrl, walPath, faultDirectory);

            await WaitForServerReadyAsync(server, baseUrl);

            using var client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl),
                Timeout = TimeSpan.FromSeconds(10)
            };

            // This request will pause before the WAL flush.
            Task<HttpResponseMessage> putTask = client.PutAsync(
                "/kv/user:1",
                new ByteArrayContent(
                    Encoding.UTF8.GetBytes("Alice")
                )
            );

            // Wait until the server reaches the checkpoint.
            using var timeout = new CancellationTokenSource(
                TimeSpan.FromSeconds(10)
            );

            while (!File.Exists(reachedPath))
            {
                timeout.Token.ThrowIfCancellationRequested();
                await Task.Delay(20, timeout.Token);
            }

            Assert.False(putTask.IsCompleted);

            // Kill the process while the PUT is suspended.
            await StopServerAsync(server);
            server = null;

            // Observe any HTTP failure to avoid an unobserved task.
            try
            {
                using var response = await putTask;
            }
            catch (HttpRequestException)
            {
                // Expected if the connection is interrupted.
            }
            catch (TaskCanceledException)
            {
                // A disconnected request may be cancelled.
            }

            // Disable fault injection before restarting.
            File.Delete(enabledPath);

            server = StartServer(baseUrl, walPath);

            await WaitForServerReadyAsync(server, baseUrl);

            // The database must recover without corrupting the WAL.
            using var healthResponse =
                await client.GetAsync("/health");

            Assert.Equal(
                HttpStatusCode.OK,
                healthResponse.StatusCode
            );

            // The interrupted write has an uncertain outcome.
            // It may or may not be present after recovery.
            using var getResponse =
                await client.GetAsync("/kv/user:1");

            Assert.True(
                getResponse.StatusCode == HttpStatusCode.OK ||
                getResponse.StatusCode == HttpStatusCode.NotFound
            );
        }
        finally
        {
            if (server is not null)
                await StopServerAsync(server);

            if (Directory.Exists(testDirectory))
                Directory.Delete(testDirectory, recursive: true);
        }
    }
}
