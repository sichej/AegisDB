using System.Diagnostics;

namespace AegisDB.Tests;

/// <summary>
/// Integration tests that verify database recovery after
/// unexpected process termination.
/// </summary>
public sealed class CrashRecoveryTests
{
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
        startInfo.ArgumentList.Add("http://127.0.0.1:5100");

        using var process = new Process
        {
            StartInfo = startInfo
        };

        try
        {
            Assert.True(process.Start());

            await WaitForServerReadyAsync(
                process,
                "http://127.0.0.1:5100"
            );

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
}