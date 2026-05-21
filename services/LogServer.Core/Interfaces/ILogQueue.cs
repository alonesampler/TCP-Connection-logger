using LogServer.Core.Entities;

namespace LogServer.Core.Interfaces;
public interface ILogQueue
{
    ValueTask EnqueueAsync(LogEntry entry, CancellationToken ct = default);
    IAsyncEnumerable<LogEntry> ReadAllAsync(CancellationToken ct = default);
    void Complete();
}
