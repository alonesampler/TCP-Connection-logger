using System.Text;
using LogServer.Core.Interfaces;

namespace LogServer.Infrastructure.Writer;

public class FileLogWriter(ILogQueue queue, string filePath) : ILogWriter
{
    private long _totalWritten = 0;

    public async Task RunAsync(CancellationToken ct = default)
    {
        await using var stream = new FileStream(
            filePath,
            FileMode.Append,
            FileAccess.Write,
            FileShare.Read,
            bufferSize: 65536,
            useAsync: true);

        await using var writer = new StreamWriter(stream, Encoding.UTF8);

        Console.WriteLine($"[Writer] Started, target: {Path.GetFullPath(filePath)}");

        try
        {
            var counter = 0;

            await foreach (var entry in queue.ReadAllAsync(ct))
            {
                await writer.WriteLineAsync(entry.ToString());
                counter++;

                if (counter >= 5)
                {
                    await writer.FlushAsync(ct);
                    counter = 0;
                }
            }
        }
        catch (OperationCanceledException) { }
        finally
        {
            Console.WriteLine($"[Writer] Stopped. Total messages: {_totalWritten:N0}");
        }
    }
}