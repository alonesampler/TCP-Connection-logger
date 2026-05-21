using Xunit;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System;
using System.Collections.Generic;
using System.Threading;
using LogServer.Infrastructure.Networking;
using LogServer.Infrastructure.Queue;
using LogServer.Infrastructure.Writer;

namespace LogServer.Tests;

public class ServerTests
{
    [Fact]
    public async Task Test_ServerReceivesMessage()
    {
        // Arrange
        var testFile = $"test_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9005);

        using var cts = new CancellationTokenSource();

        var writerTask = writer.RunAsync(cts.Token);
        var serverTask = server.RunAsync(cts.Token);

        await Task.Delay(500);

        // Act
        using var client = new TcpClient();
        await client.ConnectAsync("localhost", 9005);
        var stream = new StreamWriter(client.GetStream()) { AutoFlush = true };
        await stream.WriteLineAsync("Hello Test");

        await Task.Delay(500);

        // Assert
        cts.Cancel();
        await Task.WhenAny(writerTask, serverTask);

        var lines = File.ReadAllLines(testFile);
        Assert.Contains(lines, l => l.Contains("Hello Test"));

        File.Delete(testFile);
    }

    [Fact]
    public async Task Test_MultipleClients()
    {
        // Arrange
        var testFile = $"test_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9006);

        using var cts = new CancellationTokenSource();

        _ = writer.RunAsync(cts.Token);
        _ = server.RunAsync(cts.Token);

        await Task.Delay(500);

        // Act
        var clients = new List<Task>();
        for (int i = 0; i < 10; i++)
        {
            clients.Add(Task.Run(async () =>
            {
                using var client = new TcpClient();
                await client.ConnectAsync("localhost", 9006);
                var stream = new StreamWriter(client.GetStream()) { AutoFlush = true };
                await stream.WriteLineAsync($"Message from client {i}");
            }));
        }

        await Task.WhenAll(clients);
        await Task.Delay(1000);

        // Assert
        cts.Cancel();
        await Task.Delay(500);

        var lines = File.ReadAllLines(testFile);
        Assert.Equal(10, lines.Length);

        File.Delete(testFile);
    }

    // Дополнительные тесты для полного покрытия ТЗ

    [Fact]
    public async Task Test_HighLoad_Throughput()
    {
        // Проверка производительности - 10_000 сообщений от 100 клиентов
        var testFile = $"test_throughput_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9010);

        using var cts = new CancellationTokenSource();
        _ = writer.RunAsync(cts.Token);
        _ = server.RunAsync(cts.Token);
        await Task.Delay(1000);

        var tasks = new List<Task>();
        var messageCount = 10_000;
        var clientsCount = 100;
        var received = 0;

        for (int c = 0; c < clientsCount; c++)
        {
            var clientId = c;
            tasks.Add(Task.Run(async () =>
            {
                using var client = new TcpClient();
                await client.ConnectAsync("localhost", 9010);
                var stream = new StreamWriter(client.GetStream()) { AutoFlush = true };
                for (int i = 0; i < messageCount / clientsCount; i++)
                {
                    await stream.WriteLineAsync($"Load test msg {clientId}:{i}");
                    Interlocked.Increment(ref received);
                }
            }));
        }

        await Task.WhenAll(tasks);
        await Task.Delay(3000); // Даем время очереди и записи

        cts.Cancel();
        await Task.Delay(500);

        var lines = File.ReadAllLines(testFile);
        Assert.Equal(messageCount, lines.Length);
        File.Delete(testFile);
    }

    [Fact]
    public async Task Test_QueueFullBehavior_DropOldest()
    {
        // Проверка поведения при переполнении очереди (DropOldest)
        var testFile = $"test_queue_full_{Guid.NewGuid()}.txt";
        var queue = new LogQueue(); // 10_000 лимит
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9011);

        using var cts = new CancellationTokenSource();
        _ = server.RunAsync(cts.Token);

        // Не запускаем writer - очередь должна переполниться
        await Task.Delay(1000);

        var client = new TcpClient();
        await client.ConnectAsync("localhost", 9011);
        var stream = new StreamWriter(client.GetStream()) { AutoFlush = true };

        // Отправляем больше 10_000 сообщений
        for (int i = 0; i < 12_000; i++)
        {
            await stream.WriteLineAsync($"Msg {i}");
        }

        await Task.Delay(2000);

        // Теперь запускаем writer
        var writerTask = writer.RunAsync(cts.Token);
        await Task.Delay(3000);

        cts.Cancel();

        var lines = File.ReadAllLines(testFile);
        // В файле не больше 10_000 строк (старые отброшены)
        Assert.True(lines.Length <= 10_000);
        File.Delete(testFile);
    }

    [Fact]
    public async Task Test_ClientDisconnect_KeepAlive()
    {
        // Проверка обработки внезапного отключения
        var testFile = $"test_disconnect_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9012);

        using var cts = new CancellationTokenSource();
        _ = writer.RunAsync(cts.Token);
        _ = server.RunAsync(cts.Token);
        await Task.Delay(1000);

        using var client = new TcpClient();
        await client.ConnectAsync("localhost", 9012);
        var stream = new StreamWriter(client.GetStream()) { AutoFlush = true };

        await stream.WriteLineAsync("Before disconnect");

        // Жестко закрываем сокет без graceful shutdown
        client.Client.Disconnect(false);

        await Task.Delay(1000);

        // Проверяем, что сервер жив и принимает нового клиента
        using var client2 = new TcpClient();
        await client2.ConnectAsync("localhost", 9012);
        var stream2 = new StreamWriter(client2.GetStream()) { AutoFlush = true };
        await stream2.WriteLineAsync("After disconnect");

        await Task.Delay(1000);

        cts.Cancel();
        await Task.Delay(500);

        var lines = File.ReadAllLines(testFile);
        Assert.Contains(lines, l => l.Contains("Before disconnect"));
        Assert.Contains(lines, l => l.Contains("After disconnect"));
        File.Delete(testFile);
    }

    [Fact]
    public async Task Test_MemoryAllocation_SpanUsage()
    {
        // Проверка, что нет лишних аллокаций (косвенно через нормальную работу с большими сообщениями)
        var testFile = $"test_memory_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9013);

        using var cts = new CancellationTokenSource();
        _ = writer.RunAsync(cts.Token);
        _ = server.RunAsync(cts.Token);
        await Task.Delay(1000);

        using var client = new TcpClient();
        await client.ConnectAsync("localhost", 9013);
        var stream = client.GetStream();

        // Большое сообщение (проверяем, что сервер не упадет)
        var largeMessage = new string('A', 100_000) + "\n";
        var data = Encoding.UTF8.GetBytes(largeMessage);
        await stream.WriteAsync(data);

        // Несколько средних сообщений
        for (int i = 0; i < 1000; i++)
        {
            var msg = $"Message {i} with some content\n";
            await stream.WriteAsync(Encoding.UTF8.GetBytes(msg));
        }

        await Task.Delay(2000);

        cts.Cancel();
        await Task.Delay(500);

        var lines = File.ReadAllLines(testFile);
        Assert.Contains(lines, l => l.Contains("AAAAAAAAA"));
        Assert.Equal(1001, lines.Length);
        File.Delete(testFile);
    }

    [Fact]
    public async Task Test_BadData_ShouldNotCrash()
    {
        // Arrange
        var testFile = $"test_{Guid.NewGuid()}.txt";
        var queue = new LogQueue();
        var writer = new FileLogWriter(queue, testFile);
        var server = new TcpConnectServer(queue, 9007);

        using var cts = new CancellationTokenSource();

        _ = writer.RunAsync(cts.Token);
        _ = server.RunAsync(cts.Token);

        await Task.Delay(500);

        // Act
        using var client = new TcpClient();
        await client.ConnectAsync("localhost", 9007);
        var stream = client.GetStream();

        var badData = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x0A };
        await stream.WriteAsync(badData);

        // Отправляем нормальное сообщение
        var normalData = Encoding.UTF8.GetBytes("After bad data\n");
        await stream.WriteAsync(normalData);

        await Task.Delay(1000);

        // Assert
        cts.Cancel();
        await Task.Delay(500);

        var lines = File.ReadAllLines(testFile);
        Assert.Contains(lines, l => l.Contains("After bad data"));

        File.Delete(testFile);
    }
}