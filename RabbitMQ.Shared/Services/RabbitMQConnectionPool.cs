using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Shared.Options;

namespace RabbitMQ.Shared.Services;

/// <summary>
/// RabbitMQ连接池类
/// 负责管理RabbitMQ连接和通道的复用，避免频繁创建和销毁连接
/// 
/// 设计原则：
/// 1. 连接是重量级资源（TCP连接），应尽量复用
/// 2. 通道是轻量级资源，可按需创建，但也要复用
/// 3. 使用ConcurrentQueue管理空闲通道池
/// 4. 线程安全：所有操作都使用线程安全的数据结构
/// 
/// 连接池策略：
/// - 单连接多通道：一个TCP连接上创建多个通道
/// - 通道池：维护一个空闲通道队列，按需分配和回收
/// - 自动清理：检测无效通道并自动清理
/// - 连接恢复：连接断开时自动重新连接
/// 
/// 使用方式：
/// 通过依赖注入获取实例：
/// builder.Services.AddSingleton<RabbitMQConnectionPool>();
/// 
/// 需要同时注册 RabbitMQConnectionFactory：
/// builder.Services.AddSingleton<RabbitMQConnectionFactory>();
/// </summary>
public class RabbitMQConnectionPool : IDisposable
{
    /// <summary>
    /// 日志记录器
    /// </summary>
    private readonly ILogger<RabbitMQConnectionPool> _logger;

    /// <summary>
    /// RabbitMQ连接工厂
    /// 用于创建新连接（从配置读取连接参数）
    /// </summary>
    private readonly RabbitMQConnectionFactory _connectionFactory;

    /// <summary>
    /// RabbitMQ连接实例
    /// </summary>
    private IConnection _connection;

    /// <summary>
    /// 空闲通道池
    /// 使用ConcurrentQueue实现线程安全的队列操作
    /// </summary>
    private readonly ConcurrentQueue<IModel> _channelPool = new();

    /// <summary>
    /// 最大通道数
    /// 超过此数量时，新请求将创建临时通道（使用后直接释放）
    /// </summary>
    public int MaxChannels { get; } = 10;

    /// <summary>
    /// 当前活动通道数
    /// </summary>
    public int ActiveChannels => _channelPool.Count;

    /// <summary>
    /// 是否已释放资源
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// 连接锁
    /// 用于线程安全的连接创建
    /// </summary>
    private readonly object _connectionLock = new();

    /// <summary>
    /// 默认构造函数（使用默认配置）
    /// 用于不支持依赖注入的场景
    /// </summary>
    public RabbitMQConnectionPool()
        : this(new RabbitMQConnectionFactory(Microsoft.Extensions.Options.Options.Create(new RabbitMQOptions())))
    {
    }

    /// <summary>
    /// 构造函数（支持依赖注入）
    /// </summary>
    /// <param name="connectionFactory">RabbitMQ连接工厂</param>
    /// <param name="logger">日志记录器（可选）</param>
    public RabbitMQConnectionPool(RabbitMQConnectionFactory connectionFactory, ILogger<RabbitMQConnectionPool>? logger = null)
    {
        _connectionFactory = connectionFactory;
        _logger = logger ?? NullLogger<RabbitMQConnectionPool>.Instance;
        _connection = CreateConnection();
        _logger.LogInformation("[RabbitMQConnectionPool] 连接池已初始化，最大通道数: {MaxChannels}", MaxChannels);
    }

    /// <summary>
    /// 创建RabbitMQ连接
    /// 使用注入的RabbitMQConnectionFactory创建连接，连接参数从配置文件读取
    /// </summary>
    /// <returns>IConnection实例</returns>
    private IConnection CreateConnection()
    {
        var connection = _connectionFactory.CreateConnection();
        
        // 订阅连接关闭事件，用于重新连接
        connection.ConnectionShutdown += OnConnectionShutdown;
        connection.CallbackException += OnCallbackException;
        
        _logger.LogInformation("[RabbitMQConnectionPool] 新连接已创建");
        return connection;
    }

    /// <summary>
    /// 确保连接有效
    /// 如果连接已关闭或断开，自动重新连接
    /// </summary>
    private void EnsureConnection()
    {
        if (_connection == null || !_connection.IsOpen)
        {
            lock (_connectionLock)
            {
                if (_connection == null || !_connection.IsOpen)
                {
                    _logger.LogWarning("[RabbitMQConnectionPool] 连接已断开，正在重新连接...");
                    
                    // 清理旧连接
                    if (_connection != null)
                    {
                        try
                        {
                            _connection.ConnectionShutdown -= OnConnectionShutdown;
                            _connection.CallbackException -= OnCallbackException;
                            _connection.Dispose();
                        }
                        catch (Exception ex)
                        {
                            _logger.LogError(ex, "[RabbitMQConnectionPool] 清理旧连接时出错");
                        }
                    }
                    
                    // 创建新连接（使用配置的连接参数）
                    _connection = CreateConnection();
                    
                    // 清空通道池（旧通道已无效）
                    while (_channelPool.TryDequeue(out var channel))
                    {
                        try
                        {
                            channel.Dispose();
                        }
                        catch { }
                    }
                    
                    _logger.LogInformation("[RabbitMQConnectionPool] 重新连接成功");
                }
            }
        }
    }

    /// <summary>
    /// 获取通道实例
    /// 优先从通道池中获取空闲通道，若没有则创建新通道
    /// </summary>
    /// <returns>IModel通道实例</returns>
    /// <exception cref="ObjectDisposedException">连接池已释放时抛出</exception>
    public IModel GetChannel()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMQConnectionPool));
        }

        EnsureConnection();

        // 尝试从通道池获取空闲通道
        if (_channelPool.TryDequeue(out var channel))
        {
            // 检查通道是否有效
            if (channel.IsOpen)
            {
                _logger.LogDebug("[RabbitMQConnectionPool] 从通道池获取通道，剩余空闲通道: {Count}", _channelPool.Count);
                return channel;
            }
            // 无效通道，直接释放
            try
            {
                channel.Dispose();
            }
            catch { }
        }

        // 创建新通道
        var newChannel = _connection.CreateModel();
        _logger.LogDebug("[RabbitMQConnectionPool] 创建新通道，当前空闲通道: {Count}", _channelPool.Count);
        return newChannel;
    }

    /// <summary>
    /// 归还通道到通道池
    /// 只有当通道池未满时才归还，否则直接释放
    /// </summary>
    /// <param name="channel">要归还的通道</param>
    public void ReturnChannel(IModel channel)
    {
        if (_disposed || channel == null)
        {
            try
            {
                channel?.Dispose();
            }
            catch { }
            return;
        }

        // 检查通道是否有效
        if (!channel.IsOpen)
        {
            try
            {
                channel.Dispose();
            }
            catch { }
            _logger.LogDebug("[RabbitMQConnectionPool] 通道已关闭，直接释放");
            return;
        }

        // 通道池未满时归还，否则释放
        if (_channelPool.Count < MaxChannels)
        {
            _channelPool.Enqueue(channel);
            _logger.LogDebug("[RabbitMQConnectionPool] 通道已归还，当前空闲通道: {Count}", _channelPool.Count);
        }
        else
        {
            try
            {
                channel.Dispose();
            }
            catch { }
            _logger.LogDebug("[RabbitMQConnectionPool] 通道池已满，直接释放通道");
        }
    }

    /// <summary>
    /// 获取连接实例
    /// </summary>
    /// <returns>IConnection连接实例</returns>
    /// <exception cref="ObjectDisposedException">连接池已释放时抛出</exception>
    public IConnection GetConnection()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMQConnectionPool));
        }

        EnsureConnection();
        return _connection;
    }

    /// <summary>
    /// 连接关闭事件处理
    /// </summary>
    private void OnConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        _logger.LogWarning("[RabbitMQConnectionPool] 连接已关闭，原因: {Reason}", e.ReplyText);
    }

    /// <summary>
    /// 回调异常事件处理
    /// </summary>
    private void OnCallbackException(object? sender, CallbackExceptionEventArgs e)
    {
        _logger.LogError(e.Exception, "[RabbitMQConnectionPool] 回调异常");
    }

    /// <summary>
    /// 释放资源
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// 释放资源（受保护方法）
    /// </summary>
    /// <param name="disposing">是否正在释放托管资源</param>
    protected virtual void Dispose(bool disposing)
    {
        if (_disposed) return;

        if (disposing)
        {
            // 释放所有通道
            while (_channelPool.TryDequeue(out var channel))
            {
                try
                {
                    channel.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[RabbitMQConnectionPool] 释放通道时出错");
                }
            }

            // 释放连接
            if (_connection != null)
            {
                try
                {
                    _connection.ConnectionShutdown -= OnConnectionShutdown;
                    _connection.CallbackException -= OnCallbackException;
                    _connection.Dispose();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "[RabbitMQConnectionPool] 释放连接时出错");
                }
            }

            _logger.LogInformation("[RabbitMQConnectionPool] 资源已释放");
        }

        _disposed = true;
    }

    /// <summary>
    /// 析构函数
    /// </summary>
    ~RabbitMQConnectionPool()
    {
        Dispose(false);
    }
}