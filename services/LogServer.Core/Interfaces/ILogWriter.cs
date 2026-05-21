namespace LogServer.Core.Interfaces;

public interface ILogWriter
{
    public Task RunAsync(CancellationToken ct = default);
}
