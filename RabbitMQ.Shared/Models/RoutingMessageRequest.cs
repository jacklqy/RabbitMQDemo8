namespace RabbitMQ.Shared.Models;

/// <summary>
/// 路由消息请求对象
/// </summary>
public class RoutingMessageRequest
{
    /// <summary>
    /// 消息内容
    /// </summary>
    public string Message { get; set; } = string.Empty;

    /// <summary>
    /// 路由键，用于消息路由
    /// </summary>
    public string RoutingKey { get; set; } = string.Empty;
}
