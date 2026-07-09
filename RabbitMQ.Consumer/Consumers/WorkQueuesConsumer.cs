using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Shared.Services;
using RabbitMQ.Shared.Constants;

namespace RabbitMQ.Consumer.Consumers;

/// <summary>
/// 工作队列模式（Work Queues）消费者
/// 
/// 模式原理：
/// - 多个消费者共同消费一个队列中的消息
/// - 实现负载均衡（轮询分发或公平分发）
/// - 使用默认交换器（空字符串）
/// - 路由键等于队列名称
/// 
/// 适用场景：任务分发，多个worker处理大量任务
/// 
/// 负载均衡策略：
/// - 轮询分发（默认）：消息依次分发给各个消费者
/// - 公平分发（BasicQos）：消费者处理完当前消息后才接收下一条
/// 
/// 消息处理流程：
/// 1. 从队列接收消息
/// 2. 解析消息内容和属性（MessageId、重试次数等）
/// 3. 执行业务处理（ProcessMessage）
/// 4. 业务成功：调用BasicAck确认消息
/// 5. 业务失败：调用BasicNack拒绝消息，根据重试策略决定是否重新入队
/// </summary>
public class WorkQueuesConsumer
{
    /// <summary>
    /// 启动工作队列模式消费者
    /// 
    /// 启动流程：
    /// 1. 从连接池获取通道
    /// 2. 声明队列（配置死信交换器）
    /// 3. 启用公平分发（可选）
    /// 4. 创建异步消费者（AsyncEventingBasicConsumer）
    /// 5. 订阅消息接收事件
    /// 6. 开始消费消息（autoAck: false，手动确认模式）
    /// 7. 等待用户输入退出
    /// 8. 归还通道到连接池
    /// </summary>
    /// <param name="consumerName">消费者名称，用于日志标识</param>
    /// <param name="fairDispatch">是否启用公平分发（默认true）</param>
    /// <param name="connectionPool">RabbitMQ连接池实例（从依赖注入获取）</param>
    public static void StartConsumer(string consumerName, bool fairDispatch = true, RabbitMQConnectionPool? connectionPool = null)
    {
        connectionPool ??= new RabbitMQConnectionPool();
        var channel = connectionPool.GetChannel();

        channel.QueueDeclare(
            queue: RabbitMQConstants.WorkQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", RabbitMQConstants.DeadLetterExchangeName },
                { "x-dead-letter-routing-key", RabbitMQConstants.DeadLetterQueueName }
            });

        // 启用公平分发（BasicQos）
        // BasicQos参数说明：
        // - prefetchSize: 预取消息大小（0表示无限制）
        // - prefetchCount: 预取消息数量（1表示每次只接收1条）
        // - global: 是否全局生效（false表示只对当前通道生效）
        // 
        // 公平分发原理：
        // - 消费者处理完当前消息后才接收下一条
        // - 避免某个消费者处理慢但接收了大量消息
        // - 实现真正的负载均衡
        if (fairDispatch)
        {
            // BasicQos：设置预取数量，实现公平分发
            // 参数说明：
            //   - prefetchSize: 预取消息大小（0表示无限制）
            //   - prefetchCount: 预取消息数量（1表示每次只接收1条未确认消息）
            //   - global: 是否全局生效（false表示只对当前通道生效）
            // 操作效果：
            //   - RabbitMQ在消费者确认当前消息前不会推送新消息
            //   - 实现负载均衡，避免快消费者空闲、慢消费者堆积
            // 注意事项：
            //   - 必须在手动确认模式（autoAck=false）下使用
            //   - prefetchCount设置过小会降低吞吐量，过大会导致负载不均衡
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            Console.WriteLine($"[{consumerName}] 公平分发模式: 已启用");
        }

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            var messageId = ea.BasicProperties?.MessageId ?? "N/A";
            var retryCount = RabbitMQConstants.GetRetryCount(ea.BasicProperties);
            
            Console.WriteLine($"\n[{consumerName}] 工作队列模式 - 收到消息:");
            Console.WriteLine($"  MessageId: {messageId}");
            Console.WriteLine($"  RetryCount: {retryCount}");
            Console.WriteLine($"  Content: {message}");

            try
            {
                ProcessMessage(message);

                try
                {
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息确认成功 (MessageId: {messageId})");
                }
                catch (Exception ackEx)
                {
                    Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息确认失败: {ackEx.Message}");
                }
            }
            catch (Exception ex)
                {
                    Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息处理失败: {ex.Message}");
                    
                    try
                    {
                        var shouldRequeue = RabbitMQConstants.ShouldRequeue(ea.BasicProperties);
                        var newRetryCount = retryCount + 1;
                        
                        if (shouldRequeue)
                        {
                            var nextDelay = RabbitMQConstants.GetNextRetryDelay(ea.BasicProperties);
                            Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息将重试 ({newRetryCount}/{RabbitMQConstants.MaxRetryCount})，延迟: {nextDelay}ms");
                            
                            var retryProperties = channel.CreateBasicProperties();
                            retryProperties.Headers = new Dictionary<string, object>();
                            retryProperties.Headers[RabbitMQConstants.RetryCountHeader] = newRetryCount;
                            retryProperties.Persistent = true;
                            if (!string.IsNullOrEmpty(ea.BasicProperties?.MessageId))
                            {
                                retryProperties.MessageId = ea.BasicProperties.MessageId;
                            }
                            
                            channel.BasicPublish(
                                exchange: RabbitMQConstants.DeadLetterExchangeName,
                                routingKey: RabbitMQConstants.WorkQueueRetryName,
                                basicProperties: retryProperties,
                                body: body);
                            
                            channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                            Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息已转发到重试队列，原消息已确认");
                        }
                        else
                        {
                            Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息已达到最大重试次数，将发送到死信队列");
                            channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                            Console.WriteLine($"[{consumerName}] 工作队列模式 - 消息已Nack，将进入死信队列");
                        }
                    }
                    catch (Exception nackEx)
                    {
                        Console.WriteLine($"[{consumerName}] 工作队列模式 - Nack操作失败: {nackEx.Message}");
                    }
                }
        };

        channel.BasicConsume(
            queue: RabbitMQConstants.WorkQueueName,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine($"\n[{consumerName}] 工作队列模式消费者已启动，等待消息...");
        Console.WriteLine($"[{consumerName}] 监听队列: {RabbitMQConstants.WorkQueueName}");
        Console.WriteLine($"[{consumerName}] 公平分发: {(fairDispatch ? "已启用" : "已禁用")}");
        Console.WriteLine($"[{consumerName}] 最大重试次数: {RabbitMQConstants.MaxRetryCount}");
        Console.WriteLine($"[{consumerName}] 手动确认模式: 开启");
        Console.ReadLine();

        connectionPool.ReturnChannel(channel);
    }

    /// <summary>
    /// 业务处理方法
    /// 
    /// 演示逻辑：
    /// - 如果消息内容包含"error"关键字（不区分大小写），抛出异常
    /// - 否则，输出"业务处理完成"
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <exception cref="InvalidOperationException">消息包含错误关键字时抛出</exception>
    private static void ProcessMessage(string message)
    {
        if (message.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("业务处理异常：消息包含错误关键字");
        }
        Console.WriteLine("  业务处理完成");
    }
}