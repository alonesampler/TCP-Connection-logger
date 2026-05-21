using LogServer.Core.Entities;
using LogServer.Core.Interfaces;
using System.Net;
using System.Net.Sockets;
using System.Text;

namespace LogServer.Infrastructure.Networking;

public sealed class TcpConnectServer(ILogQueue queue, int port)
{
    private const int BufferSize = 4096;
    private const int ReceiveTimeout = 30_000; // 30 sec

    public async Task RunAsync(CancellationToken ct)
    {
        var listener = new TcpListener(IPAddress.Any, port);
        listener.Start();
        Console.WriteLine($"[Server] Listening on port {port}...");

        try
        {
            while (!ct.IsCancellationRequested)
            {
                TcpClient client;
                try
                {
                    client = await listener.AcceptTcpClientAsync(ct).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { break; }

                _ = HandleClientAsync(client, ct);
            }
        }
        finally
        {
            listener.Stop();
            Console.WriteLine("[Server] Stopped.");
        }
    }

    private async Task HandleClientAsync(TcpClient tcpClient, CancellationToken ct)
    {
        var endpoint = tcpClient.Client.RemoteEndPoint?.ToString() ?? "unknown";
        var messageCount = 0;

        Console.WriteLine($"[+] Connected: {endpoint}");

        try
        {
            tcpClient.ReceiveTimeout = ReceiveTimeout;

            await using var stream = tcpClient.GetStream();
            using var reader = new StreamReader(stream, Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: BufferSize,
                leaveOpen: true);

            while (!ct.IsCancellationRequested)
            {
                string? line;
                try
                {
                    line = await reader.ReadLineAsync(ct).ConfigureAwait(false);
                }
                catch (Exception ex) when (ex is IOException or OperationCanceledException)
                {
                    break;
                }

                if (line is null) break;
                if (string.IsNullOrWhiteSpace(line)) continue;

                var entry = new LogEntry(DateTime.UtcNow, endpoint, line);
                await queue.EnqueueAsync(entry, ct).ConfigureAwait(false);
                messageCount++;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[!] Error from {endpoint}: {ex.Message}");
        }
        finally
        {
            tcpClient.Dispose();
            Console.WriteLine($"[-] Disconnected: {endpoint} (messages: {messageCount})");  // ← ПОКАЗЫВАТЬ СЧЁТЧИК
        }
    }
}
