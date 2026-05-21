namespace LogServer.Core.Entities;

public record LogEntry(
    DateTime ReceivedAt,
    string ClientId,
    string Message
)
{
    public override string ToString() =>
        $"[{ReceivedAt:yyyy-MM-dd HH:mm:ss}] [{ClientId}] {Message}";
}
