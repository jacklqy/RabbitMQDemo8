namespace RabbitMQ.Shared.Models;

/// <summary>
/// 消息请求对象
/// </summary>
public class MessageRequest
{
    /// <summary>
    /// 消息内容
    /// </summary>
    public string Message { get; set; } = string.Empty;
}
