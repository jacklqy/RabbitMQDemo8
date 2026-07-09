namespace RabbitMQ.Shared.Models;

/// <summary>
/// 主题消息请求对象
/// </summary>
public class TopicMessageRequest
{
    /// <summary>
    /// 消息内容
    /// </summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>
    /// 主题名称，支持多级路径（如 user.create, order.pay.success）
    /// </summary>
    public string Topic { get; set; } = string.Empty;
}
