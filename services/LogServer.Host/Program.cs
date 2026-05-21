using LogServer.Core.Interfaces;
using LogServer.Infrastructure.Networking;
using LogServer.Infrastructure.Queue;
using LogServer.Infrastructure.Writer;

const int port = 9000;

var logFile = Environment.GetEnvironmentVariable("LOG_FILE_PATH") ?? "/app/logs/logs.txt";

var logDirectory = Path.GetDirectoryName(logFile);
if (!string.IsNullOrEmpty(logDirectory) && !Directory.Exists(logDirectory))
{
    Directory.CreateDirectory(logDirectory);
}

ILogQueue queue = new LogQueue();
ILogWriter writer = new FileLogWriter(queue, logFile);
var server = new TcpConnectServer(queue, port);

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true;
    Console.WriteLine("\n[Host] Shutting down...");
    cts.Cancel();
};

var writerTask = writer.RunAsync(cts.Token);
var serverTask = server.RunAsync(cts.Token);

await Task.WhenAny(serverTask, Task.Delay(-1, cts.Token));

Console.WriteLine("\n[Host] Stopping server...");
cts.Cancel();
queue.Complete();                // Сигнал writer-у

try
{
    await writerTask.WaitAsync(TimeSpan.FromSeconds(10));  // Ждём с таймаутом
}
catch (TimeoutException)
{
    Console.WriteLine("[WARN] Writer timeout, forcing exit");
}

Console.WriteLine("[Host] Bye.");