namespace RabbitMQ.Shared.Options;

/// <summary>
/// RabbitMQ连接配置选项
/// 
/// 配置项说明：
/// - HostName: RabbitMQ服务器主机名或IP地址
/// - UserName: 用户名
/// - Password: 密码
/// - Port: AMQP端口（默认5672，TLS端口5671）
/// - VirtualHost: 虚拟主机（默认"/"）
/// - ConnectionTimeout: 连接超时时间（毫秒）
/// - RequestedHeartbeat: 心跳超时时间（秒）
/// </summary>
public class RabbitMQOptions
{
    /// <summary>
    /// 配置节名称
    /// </summary>
    public const string SectionName = "RabbitMQ";

    /// <summary>
    /// RabbitMQ服务器主机名或IP地址
    /// </summary>
    public string HostName { get; set; } = "localhost";

    /// <summary>
    /// 用户名
    /// </summary>
    public string UserName { get; set; } = "guest";

    /// <summary>
    /// 密码
    /// </summary>
    public string Password { get; set; } = "guest";

    /// <summary>
    /// AMQP端口
    /// 默认值：5672（非加密），5671（TLS加密）
    /// </summary>
    public int Port { get; set; } = 5672;

    /// <summary>
    /// 虚拟主机
    /// 默认值："/"
    /// </summary>
    public string VirtualHost { get; set; } = "/";

    /// <summary>
    /// 连接超时时间（毫秒）
    /// 默认值：30000（30秒）
    /// </summary>
    public int ConnectionTimeout { get; set; } = 30000;

    /// <summary>
    /// 心跳超时时间（秒）
    /// 默认值：60秒
    /// </summary>
    public ushort RequestedHeartbeat { get; set; } = 60;

    /// <summary>
    /// 通道池最大通道数
    /// 默认值：10
    /// </summary>
    public int MaxChannels { get; set; } = 10;

    /// <summary>
    /// 最大重试次数
    /// 默认值：3
    /// </summary>
    public int MaxRetryCount { get; set; } = 3;

    /// <summary>
    /// 重试退避时间（毫秒）
    /// 默认值：1000ms
    /// </summary>
    public int RetryBackoffMs { get; set; } = 1000;
}