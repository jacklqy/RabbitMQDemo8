using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Shared.Services;

/// <summary>
/// RabbitMQ核心服务类
/// 负责管理RabbitMQ连接、通道和发布确认机制
/// 
/// 核心特性：
/// 1. 发布确认机制（Publisher Confirms）：确保消息到达RabbitMQ服务器后才返回
/// 2. 连接管理：自动创建和管理连接生命周期
/// 3. 通道管理：维护单一通道实例，支持消息发布和队列操作
/// 4. 资源清理：实现IDisposable接口，确保资源正确释放
/// 5. 结构化日志：使用ILogger记录日志，便于问题排查
/// 
/// 使用方式：
/// 通过依赖注入获取实例：
/// builder.Services.AddSingleton<RabbitMQService>();
/// 
/// 需要同时注册 RabbitMQConnectionFactory：
/// builder.Services.AddSingleton<RabbitMQConnectionFactory>();
/// </summary>
public class RabbitMQService : IDisposable
{
    /// <summary>
    /// 日志记录器
    /// </summary>
    private readonly ILogger<RabbitMQService> _logger;

    /// <summary>
    /// RabbitMQ连接实例
    /// 连接是RabbitMQ客户端与服务器之间的TCP连接
    /// 应尽量复用连接，避免频繁创建和销毁
    /// </summary>
    private readonly IConnection _connection;

    /// <summary>
    /// RabbitMQ通道实例
    /// 通道是建立在连接之上的虚拟连接，用于执行所有操作
    /// 每个通道独立维护消息序列号（deliveryTag）
    /// </summary>
    private readonly IModel _channel;

    /// <summary>
    /// 待确认消息字典（线程安全）
    /// Key: deliveryTag（消息序列号，由RabbitMQ服务器分配）
    /// Value: TaskCompletionSource（用于异步等待确认结果）
    /// 
    /// 工作原理：
    /// 1. 发布消息前，创建TaskCompletionSource并记录到字典
    /// 2. 等待TaskCompletionSource的Task完成（超时5秒）
    /// 3. 收到BasicAck事件时，设置TaskCompletionSource为成功
    /// 4. 收到BasicNack事件时，设置TaskCompletionSource为失败
    /// </summary>
    private readonly ConcurrentDictionary<ulong, TaskCompletionSource<bool>> _pendingConfirms = new();

    /// <summary>
    /// 是否已释放资源
    /// 防止重复释放
    /// </summary>
    private bool _disposed;

    /// <summary>
    /// 构造函数，初始化RabbitMQ连接和通道
    /// 
    /// 初始化流程：
    /// 1. 通过注入的RabbitMQConnectionFactory创建RabbitMQ连接
    /// 2. 创建通道（IModel）
    /// 3. 启用发布确认模式（ConfirmSelect）
    /// 4. 订阅确认事件（BasicAcks/BasicNacks）
    /// 5. 订阅连接关闭事件（ConnectionShutdown）
    /// </summary>
    /// <param name="connectionFactory">RabbitMQ连接工厂（从配置读取连接参数）</param>
    /// <param name="logger">日志记录器（可选）</param>
    public RabbitMQService(RabbitMQConnectionFactory connectionFactory, ILogger<RabbitMQService>? logger = null)
    {
        _logger = logger ?? NullLogger<RabbitMQService>.Instance;
        _connection = connectionFactory.CreateConnection();
        _channel = _connection.CreateModel();

        /// <summary>
        /// 启用发布确认模式
        /// 调用此方法后，RabbitMQ会在消息持久化到磁盘后发送确认
        /// 只有收到确认，才意味着消息已安全存储
        /// 
        /// 发布确认的三种模式：
        /// 1. 单条确认（逐消息等待确认）：简单但性能最差
        /// 2. 批量确认（累计多条后统一确认）：性能较好，但消息丢失时难以定位
        /// 3. 异步确认（通过事件回调）：性能最佳，本实现采用此模式
        /// </summary>
        _channel.ConfirmSelect();

        /// <summary>
        /// 订阅消息确认事件
        /// 当RabbitMQ确认消息已接收时触发
        /// 参数说明：
        ///   ea.DeliveryTag: 消息序列号
        ///   ea.Multiple: 是否为批量确认（true表示确认所有小于等于此tag的消息）
        /// </summary>
        _channel.BasicAcks += OnBasicAck;

        /// <summary>
        /// 订阅消息拒绝事件
        /// 当RabbitMQ拒绝消息时触发（消息未被路由或服务器错误）
        /// 参数说明：
        ///   ea.DeliveryTag: 消息序列号
        ///   ea.Multiple: 是否为批量拒绝
        /// </summary>
        _channel.BasicNacks += OnBasicNack;

        /// <summary>
        /// 订阅连接关闭事件
        /// 当连接意外断开时触发，用于清理待确认消息
        /// </summary>
        _connection.ConnectionShutdown += OnConnectionShutdown;

        _logger.LogInformation("[RabbitMQService] 服务已初始化，发布确认模式已启用");
    }

    /// <summary>
    /// 获取RabbitMQ通道实例
    /// </summary>
    /// <returns>IModel通道实例</returns>
    /// <exception cref="ObjectDisposedException">服务已释放时抛出</exception>
    public IModel GetChannel()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMQService));
        }
        return _channel;
    }

    /// <summary>
    /// 获取RabbitMQ连接实例
    /// </summary>
    /// <returns>IConnection连接实例</returns>
    /// <exception cref="ObjectDisposedException">服务已释放时抛出</exception>
    public IConnection GetConnection()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(RabbitMQService));
        }
        return _connection;
    }

    /// <summary>
    /// 发布消息（带发布确认机制）
    /// 使用异步方式等待消息确认，确保消息到达RabbitMQ服务器
    /// 
    /// 发布确认流程：
    /// 1. 创建TaskCompletionSource，用于等待确认
    /// 2. 获取下一个消息序列号（NextPublishSeqNo）
    /// 3. 将序列号和TaskCompletionSource记录到待确认字典
    /// 4. 调用BasicPublish发送消息
    /// 5. 等待TaskCompletionSource完成（超时5秒）
    /// 6. 根据确认结果返回或抛出异常
    /// 
    /// 可靠性保证：
    /// - 消息持久化：通过设置BasicProperties.Persistent = true
    /// - 发布确认：只有收到RabbitMQ确认才返回成功
    /// - 超时机制：5秒超时，防止无限等待
    /// </summary>
    /// <param name="exchange">交换器名称（空字符串表示默认交换器）</param>
    /// <param name="routingKey">路由键（用于消息路由）</param>
    /// <param name="body">消息体（字节数组）</param>
    /// <param name="properties">消息属性（可选，包含持久化、消息ID等）</param>
    /// <returns>Task，完成时表示消息已确认</returns>
    /// <exception cref="InvalidOperationException">消息发布未被确认时抛出</exception>
    /// <exception cref="TimeoutException">等待确认超时时抛出</exception>
    public async Task PublishWithConfirmAsync(string exchange, string routingKey, byte[] body, IBasicProperties? properties = null)
    {
        var tcs = new TaskCompletionSource<bool>();
        
        var deliveryTag = _channel.NextPublishSeqNo;
        
        _pendingConfirms.TryAdd(deliveryTag, tcs);

        // BasicPublish：发布消息到指定交换器
        // 参数说明：
        //   - exchange: 交换器名称（空字符串表示默认交换器）
        //   - routingKey: 路由键，用于消息路由
        //   - properties: 消息属性（持久化、消息ID、头等）
        //   - body: 消息体（二进制数据）
        // 注意事项：
        //   - 发布确认模式下，消息发送后会等待OnBasicAck/OnBasicNack事件
        //   - 如果交换器不存在且mandatory=false，消息会被静默丢弃
        //   - 如果mandatory=true且消息无法路由，会触发BasicReturn事件
        _channel.BasicPublish(exchange, routingKey, properties, body);

        _logger.LogDebug("[RabbitMQService] 消息已发送，等待确认 (DeliveryTag: {DeliveryTag})", deliveryTag);

        var confirmed = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
        
        if (!confirmed)
        {
            _logger.LogError("[RabbitMQService] 消息发布未被确认 (DeliveryTag: {DeliveryTag})", deliveryTag);
            throw new InvalidOperationException("消息发布未被确认");
        }
    }

    /// <summary>
    /// 消息确认事件处理方法
    /// 当RabbitMQ确认消息已接收时触发
    /// 
    /// 处理逻辑：
    /// 1. 从待确认字典中移除对应的deliveryTag
    /// 2. 设置TaskCompletionSource为成功（TrySetResult(true)）
    /// 3. 记录日志
    /// </summary>
    /// <param name="model">通道模型（事件发送者）</param>
    /// <param name="ea">确认事件参数</param>
    private void OnBasicAck(object? model, BasicAckEventArgs ea)
    {
        if (_pendingConfirms.TryRemove(ea.DeliveryTag, out var tcs))
        {
            tcs.TrySetResult(true);
            _logger.LogInformation("[RabbitMQService] 消息确认成功 (DeliveryTag: {DeliveryTag}, Multiple: {Multiple})", 
                ea.DeliveryTag, ea.Multiple);
        }
    }

    /// <summary>
    /// 消息拒绝事件处理方法
    /// 当RabbitMQ拒绝消息时触发
    /// 
    /// 拒绝原因可能包括：
    /// 1. 消息未被路由到任何队列（mandatory标志设置时）
    /// 2. 服务器内部错误
    /// 3. 消息TTL过期
    /// 
    /// 处理逻辑：
    /// 1. 从待确认字典中移除对应的deliveryTag
    /// 2. 设置TaskCompletionSource为失败（TrySetResult(false)）
    /// 3. 记录日志
    /// </summary>
    /// <param name="model">通道模型（事件发送者）</param>
    /// <param name="ea">拒绝事件参数</param>
    private void OnBasicNack(object? model, BasicNackEventArgs ea)
    {
        if (_pendingConfirms.TryRemove(ea.DeliveryTag, out var tcs))
        {
            tcs.TrySetResult(false);
            _logger.LogError("[RabbitMQService] 消息确认失败 (DeliveryTag: {DeliveryTag}, Multiple: {Multiple})", 
                ea.DeliveryTag, ea.Multiple);
        }
    }

    /// <summary>
    /// 连接关闭事件处理方法
    /// 当RabbitMQ连接关闭时触发（正常关闭或异常断开）
    /// 
    /// 处理逻辑：
    /// 1. 遍历待确认字典，设置所有未确认消息为失败
    /// 2. 清空待确认字典
    /// 3. 记录日志
    /// 
    /// 防止连接断开后，待确认消息的TaskCompletionSource永远不会完成
    /// </summary>
    /// <param name="sender">发送者（连接实例）</param>
    /// <param name="e">关闭事件参数</param>
    private void OnConnectionShutdown(object? sender, ShutdownEventArgs e)
    {
        foreach (var tcs in _pendingConfirms.Values)
        {
            tcs.TrySetResult(false);
        }
        
        _pendingConfirms.Clear();
        
        _logger.LogWarning("[RabbitMQService] 连接已关闭，原因: {Reason}", e.ReplyText);
    }

    /// <summary>
    /// 释放资源（IDisposable接口实现）
    /// 
    /// 释放顺序：
    /// 1. 取消事件订阅（防止内存泄漏）
    /// 2. 关闭通道
    /// 3. 释放通道资源
    /// 4. 关闭连接
    /// 5. 释放连接资源
    /// 6. 清空待确认字典
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
            if (_channel != null)
            {
                _channel.BasicAcks -= OnBasicAck;
                _channel.BasicNacks -= OnBasicNack;
                _channel.Close();
                _channel.Dispose();
            }

            if (_connection != null)
            {
                _connection.ConnectionShutdown -= OnConnectionShutdown;
                _connection.Close();
                _connection.Dispose();
            }

            _pendingConfirms.Clear();
        }

        _disposed = true;
        _logger.LogInformation("[RabbitMQService] 资源已释放");
    }

    /// <summary>
    /// 析构函数
    /// 当对象被垃圾回收时调用
    /// 仅用于释放非托管资源
    /// </summary>
    ~RabbitMQService()
    {
        Dispose(false);
    }
}