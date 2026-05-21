using LogServer.Core.Entities;
using LogServer.Core.Interfaces;
using System.Threading.Channels;

namespace LogServer.Infrastructure.Queue;

public sealed class LogQueue : ILogQueue
{
    private readonly Channel<LogEntry> _channel = Channel.CreateBounded<LogEntry>(
            new BoundedChannelOptions(100_000)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.DropOldest,
                AllowSynchronousContinuations = false
            });

    public async ValueTask EnqueueAsync(LogEntry entry, CancellationToken ct = default)
        => await _channel.Writer.WriteAsync(entry, ct).ConfigureAwait(false);

    public IAsyncEnumerable<LogEntry> ReadAllAsync(CancellationToken ct = default)
        => _channel.Reader.ReadAllAsync(ct);

    public void Complete()
        => _channel.Writer.Complete();
}