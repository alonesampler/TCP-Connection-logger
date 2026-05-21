namespace LogServer.Core.Interfaces;
public interface ISystemLogger
{
    void Info(string message);
    void Error(string message);
    void Warning(string message);
}
