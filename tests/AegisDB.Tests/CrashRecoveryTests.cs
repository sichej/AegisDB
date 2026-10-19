using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace AegisDB.Tests;

/// <summary>
/// Integration tests that verify database recovery after
/// unexpected process termination.
/// </summary>
public sealed class CrashRecoveryTests
{

    /// <summary>
    /// Finds an available local TCP port for the test server.
    ///
    /// The port is released immediately, so another process could
    /// theoretically claim it before the server starts.
    /// </summary>
    private static int GetAvailablePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);

        listener.Start();

        int port = ((IPEndPoint)listener.LocalEndpoint).Port;

        listener.Stop();

        return port;
    }
    [Fact]
    public async Task Server_ShouldStartAsSeparateProcess()
    {
        // Locate the server project from the test execution directory.
        string serverProjectPath = Path.GetFullPath(
            Path.Combine(
                AppContext.BaseDirectory,
                "../../../../../src/AegisDB.Server/AegisDB.Server.csproj"
            )
        );

        Assert.True(
            File.Exists(serverProjectPath),
            $"Server project not found: {serverProjectPath}"
        );

        // Use a unique WAL path to avoid interfering with other tests.
        string testDirectory = Path.Combine(
            Path.GetTempPath(),
            $"aegis-test-{Guid.NewGuid():N}"
        );

        Directory.CreateDirectory(testDirectory);

        string walPath = Path.Combine(testDirectory, "test.wal");

        int port = GetAvailablePort();

        string baseUrl = $"http://127.0.0.1:{port}";

        var startInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true
        };

        startInfo.ArgumentList.Add("run");
        startInfo.ArgumentList.Add("--no-launch-profile");
        startInfo.ArgumentList.Add("--project");
        startInfo.ArgumentList.Add(serverProjectPath);
        startInfo.ArgumentList.Add("--");
        startInfo.ArgumentList.Add("--urls");
        startInfo.ArgumentList.Add(baseUrl);
        // ASP.NET Core maps double underscores to configuration sections.
        startInfo.Environment["Storage__WalPath"] = walPath;

        using var process = new Process
        {
            StartInfo = startInfo
        };

        try
        {
            Assert.True(process.Start());

            await WaitForServerReadyAsync(process, baseUrl);

            Assert.False(process.HasExited);
        }
        finally
        {
            if (!process.HasExited)
            {
                // Terminate the entire process tree.
                process.Kill(entireProcessTree: true);
            }

            await process.WaitForExitAsync();
            // Clean up only after the server process has terminated.
            if (Directory.Exists(testDirectory))
            {
                Directory.Delete(testDirectory, recursive: true);
            }
        }
    }

    /// <summary>
    /// Waits until the server responds successfully to a health check.
    /// Fails if the process exits or the startup timeout is exceeded.
    /// </summary>
    private static async Task WaitForServerReadyAsync(
        Process process,
        string baseUrl,
        CancellationToken cancellationToken = default)
    {
        using var client = new HttpClient
        {
            Timeout = TimeSpan.FromSeconds(1)
        };

        using var timeout = CancellationTokenSource
            .CreateLinkedTokenSource(cancellationToken);

        timeout.CancelAfter(TimeSpan.FromSeconds(30));

        while (true)
        {
            timeout.Token.ThrowIfCancellationRequested();

            if (process.HasExited)
            {
                throw new InvalidOperationException(
                    $"Server exited with code {process.ExitCode}."
                );
            }

            try
            {
                using var response = await client.GetAsync(
                    $"{baseUrl}/health",
                    timeout.Token
                );

                if (response.IsSuccessStatusCode)
                    return;
            }
            catch (HttpRequestException)
            {
                // The server may not be listening yet.
            }
            catch (TaskCanceledException) when (!timeout.IsCancellationRequested)
            {
                // An individual health request may time out during startup.
            }

            await Task.Delay(200, timeout.Token);
        }
    }

    [Fact]
    public async Task Server_ShouldRecoverDataAfterProcessCrash()
    {
        // Locate the compiled server assembly.
        // The test and server must use the same build configuration.
        string configuration =
#if DEBUG
                    "Debug";
#else
            "Release";
#endif

        string serverDllPath = Path.GetFullPath(
            Path.Combine(
                AppContext.BaseDirectory,
                "../../../../../src/AegisDB.Server/bin",
                configuration,
                "net10.0",
                "AegisDB.Server.dll"
            )
        );

        Assert.True(
            File.Exists(serverDllPath),
            $"Server assembly not found: {serverDllPath}"
        );

        string testDirectory = Path.Combine(
            Path.GetTempPath(),
            $"aegis-crash-{Guid.NewGuid():N}"
        );

        Directory.CreateDirectory(testDirectory);

        string walPath = Path.Combine(testDirectory, "test.wal");
        string baseUrl = $"http://127.0.0.1:{GetAvailablePort()}";

        Process StartServer()
        {
            var startInfo = new ProcessStartInfo
            {
                FileName = "dotnet",
                WorkingDirectory = Path.GetDirectoryName(serverDllPath)!,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true
            };

            // Execute the compiled server directly.
            startInfo.ArgumentList.Add(serverDllPath);
            startInfo.ArgumentList.Add("--urls");
            startInfo.ArgumentList.Add(baseUrl);

            // Preserve the same WAL across process restarts.
            startInfo.Environment["Storage__WalPath"] = walPath;
            startInfo.Environment["ASPNETCORE_ENVIRONMENT"] = "Testing";

            var process = Process.Start(startInfo)
            ?? throw new InvalidOperationException(
                "Failed to start AegisDB.Server."
            );

            // Start consuming both redirected output streams.
            CaptureProcessOutput(process);

            return process;
        }

        static async Task StopServerAsync(Process process)
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

        Process? server = null;

        try
        {
            using var client = new HttpClient
            {
                BaseAddress = new Uri(baseUrl)
            };

            // Start the first server instance.
            server = StartServer();

            await WaitForServerReadyAsync(server, baseUrl);

            using (var response = await client.PutAsync(
                "/kv/user:1",
                new ByteArrayContent(
                    Encoding.UTF8.GetBytes("Alice")
                )))
            {
                Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
            }

            // Simulate an unexpected process termination.
            await StopServerAsync(server);
            server = null;

            // Restart with the same persistent WAL.
            server = StartServer();

            await WaitForServerReadyAsync(server, baseUrl);

            // Verify the confirmed write survived the crash.
            using var getResponse = await client.GetAsync("/kv/user:1");

            Assert.Equal(HttpStatusCode.OK, getResponse.StatusCode);

            string value = await getResponse.Content.ReadAsStringAsync();

            Assert.Equal("Alice", value);
        }
        finally
        {
            if (server is not null)
                await StopServerAsync(server);

            if (Directory.Exists(testDirectory))
                Directory.Delete(testDirectory, recursive: true);
        }
    }

    /// <summary>
    /// Continuously drains the server's stdout and stderr streams.
    ///
    /// This prevents redirected output buffers from filling up
    /// and blocking the server process.
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
}