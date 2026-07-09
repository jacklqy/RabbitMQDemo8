using Microsoft.Extensions.Hosting;
using RabbitMQ.Shared.Services;
using RabbitMQ.Shared.Constants;

namespace RabbitMQ.Shared.HostedServices;

/// <summary>
/// RabbitMQ初始化托管服务
/// 在应用启动时执行队列和交换器的初始化
/// 
/// 设计目的：
/// 1. 避免在控制器构造函数中重复初始化（每次HTTP请求都会创建控制器实例）
/// 2. 确保队列和交换器在应用启动时就已存在
/// 3. 支持固定队列名称，防止消息丢失
/// 4. 实现应用启动时的一次性初始化
/// 
/// 使用方式：
/// 在Program.cs中注册：
/// builder.Services.AddHostedService<RabbitMQInitializer>();
/// 
/// 生命周期：
/// - StartAsync: 应用启动时调用，执行初始化
/// - StopAsync: 应用停止时调用，不需要特殊清理
/// </summary>
public class RabbitMQInitializer : IHostedService
{
    /// <summary>
    /// RabbitMQ服务实例
    /// 用于获取通道并执行队列初始化
    /// </summary>
    private readonly RabbitMQService _rabbitMQService;

    /// <summary>
    /// 构造函数，注入RabbitMQ服务
    /// </summary>
    /// <param name="rabbitMQService">RabbitMQ服务实例</param>
    public RabbitMQInitializer(RabbitMQService rabbitMQService)
    {
        _rabbitMQService = rabbitMQService;
    }

    /// <summary>
    /// 启动服务时执行
    /// 初始化所有队列和交换器
    /// 
    /// 初始化流程：
    /// 1. 获取RabbitMQ通道实例
    /// 2. 调用RabbitMQConstants.InitializeAllQueues初始化所有队列和交换器
    /// 3. 记录初始化完成日志
    /// 
    /// 为什么在启动时初始化：
    /// - 队列和交换器必须在消息发送前存在
    /// - 使用固定名称队列，确保消息可路由到队列
    /// - 消费者未启动时，消息可存储在队列中
    /// - 后续消费者启动后可消费累积的消息
    /// </summary>
    /// <param name="cancellationToken">取消令牌（用于优雅关闭）</param>
    /// <returns>Task</returns>
    public Task StartAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("[RabbitMQInitializer] 开始初始化RabbitMQ队列和交换器...");
        
        // 获取RabbitMQ通道实例
        var channel = _rabbitMQService.GetChannel();
        
        // 初始化所有队列和交换器
        // 此方法是幂等的，队列或交换器已存在时不会重新创建
        RabbitMQConstants.InitializeAllQueues(channel);
        
        Console.WriteLine("[RabbitMQInitializer] RabbitMQ队列和交换器初始化完成");
        
        return Task.CompletedTask;
    }

    /// <summary>
    /// 停止服务时执行
    /// 
    /// 不需要特殊清理的原因：
    /// - RabbitMQService已实现IDisposable接口
    /// - 服务容器会自动调用RabbitMQService.Dispose()
    /// - 队列和交换器在RabbitMQ服务器上持久化存储
    /// </summary>
    /// <param name="cancellationToken">取消令牌（用于优雅关闭）</param>
    /// <returns>Task</returns>
    public Task StopAsync(CancellationToken cancellationToken)
    {
        Console.WriteLine("[RabbitMQInitializer] 服务停止");
        return Task.CompletedTask;
    }
}
