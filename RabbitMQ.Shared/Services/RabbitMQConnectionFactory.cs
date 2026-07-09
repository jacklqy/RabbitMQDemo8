using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Shared.Options;

namespace RabbitMQ.Shared.Services;

/// <summary>
/// RabbitMQ连接工厂类
/// 负责创建和管理RabbitMQ连接
/// 
/// 连接创建原则：
/// 1. 连接是重量级资源，应尽量复用
/// 2. 连接工厂是轻量级对象，可按需创建
/// 3. 连接配置通过RabbitMQOptions从配置文件读取
/// 
/// 使用方式：
/// 通过依赖注入获取实例：
/// builder.Services.AddSingleton<RabbitMQConnectionFactory>();
/// 
/// 在配置文件中配置：
/// "RabbitMQ": {
///   "HostName": "localhost",
///   "UserName": "guest",
///   "Password": "guest",
///   "Port": 5672
/// }
/// </summary>
public class RabbitMQConnectionFactory
{
    private readonly RabbitMQOptions _options;

    /// <summary>
    /// 创建RabbitMQ连接工厂实例（支持依赖注入）
    /// 使用IOptions<T>模式从配置文件读取RabbitMQ连接参数
    /// </summary>
    /// <param name="options">RabbitMQ配置选项（通过IOptions包装）</param>
    public RabbitMQConnectionFactory(IOptions<RabbitMQOptions> options)
    {
        _options = options.Value;
    }

    /// <summary>
    /// 创建RabbitMQ连接
    /// 
    /// 连接配置说明：
    /// - HostName: RabbitMQ服务器地址（从配置读取）
    /// - UserName: 用户名（从配置读取）
    /// - Password: 密码（从配置读取）
    /// - Port: 端口号（从配置读取）
    /// - VirtualHost: 虚拟主机（从配置读取）
    /// - ConnectionTimeout: 连接超时时间（从配置读取）
    /// - RequestedHeartbeat: 心跳超时时间（从配置读取）
    /// - DispatchConsumersAsync: 是否启用异步消费者调度
    ///   - true: 消费者回调在ThreadPool线程中执行，支持async/await
    ///   - false: 消费者回调在I/O线程中执行，不支持await
    /// 
    /// 连接创建流程：
    /// 1. 创建ConnectionFactory实例
    /// 2. 设置连接参数（从RabbitMQOptions读取）
    /// 3. 调用CreateConnection创建连接
    /// 4. 返回连接实例
    /// </summary>
    /// <returns>IConnection实例</returns>
    public IConnection CreateConnection()
    {
        var factory = new ConnectionFactory
        {
            HostName = _options.HostName,
            UserName = _options.UserName,
            Password = _options.Password,
            Port = _options.Port,
            VirtualHost = _options.VirtualHost,
            RequestedHeartbeat = TimeSpan.FromSeconds(_options.RequestedHeartbeat),
            DispatchConsumersAsync = true
        };

        var connection = factory.CreateConnection();
        Console.WriteLine($"[RabbitMQ] 连接已建立 (Host: {_options.HostName}, Port: {_options.Port}, VirtualHost: {_options.VirtualHost})");

        return connection;
    }
}