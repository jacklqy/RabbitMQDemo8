using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Shared.Services;
using RabbitMQ.Shared.Constants;

namespace RabbitMQ.Consumer.Consumers;

/// <summary>
/// 路由模式（Routing - Direct）消费者
/// 
/// 模式原理：
/// - 使用Direct交换器，根据精确匹配的路由键将消息路由到对应队列
/// - 一个队列可绑定多个路由键
/// - 消息只发送到匹配的队列
/// 
/// 适用场景：日志分级处理（error/info/warning）、按类型分发消息
/// 
/// 队列配置：
/// - direct_error_queue: 绑定"error"，只接收error消息
/// - direct_info_queue: 绑定"info"，只接收info消息
/// - direct_warning_queue: 绑定"warning"，只接收warning消息
/// - direct_all_queue: 绑定"error"、"info"、"warning"，接收所有消息
/// 
/// 消息处理流程：
/// 1. 声明Direct交换器
/// 2. 声明队列（配置死信交换器）
/// 3. 将队列绑定到Direct交换器（根据路由键）
/// 4. 从队列接收消息
/// 5. 执行业务处理
/// 6. 业务成功：确认消息；业务失败：拒绝消息
/// </summary>
public class RoutingConsumer
{
    /// <summary>
    /// 启动路由模式消费者
    /// 
    /// 启动流程：
    /// 1. 从连接池获取通道
    /// 2. 声明Direct交换器（幂等操作）
    /// 3. 声明队列（配置死信交换器）
    /// 4. 将队列绑定到Direct交换器（绑定所有路由键）
    /// 5. 创建异步消费者
    /// 6. 订阅消息接收事件
    /// 7. 开始消费消息
    /// 8. 等待用户输入退出
    /// 9. 归还通道到连接池
    /// </summary>
    /// <param name="consumerName">消费者名称，用于日志标识</param>
    /// <param name="queueName">要监听的队列名称</param>
    /// <param name="connectionPool">RabbitMQ连接池实例（从依赖注入获取）</param>
    public static void StartConsumer(string consumerName, string queueName, RabbitMQConnectionPool? connectionPool = null)
    {
        connectionPool ??= new RabbitMQConnectionPool();
        var channel = connectionPool.GetChannel();

        // 声明Direct交换器
        // 参数说明：
        // - exchange: 交换器名称
        // - type: ExchangeType.Direct - 精确匹配路由键
        // - durable: true - 交换器持久化
        // - autoDelete: false - 不自动删除
        channel.ExchangeDeclare(
            exchange: RabbitMQConstants.DirectExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        var retryQueueName = RabbitMQConstants.GetRetryQueueName(queueName);

        // 死信路由键设置为死信队列名称，确保超过重试次数的消息直接进入死信队列
        channel.QueueDeclare(
            queue: queueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", RabbitMQConstants.DeadLetterExchangeName },
                { "x-dead-letter-routing-key", RabbitMQConstants.DeadLetterQueueName }
            });

        // 获取队列绑定的所有路由键
        var routingKeys = RabbitMQConstants.GetDirectRoutingKeys(queueName);

        // 将队列绑定到Direct交换器（一个队列可绑定多个路由键）
        foreach (var routingKey in routingKeys)
        {
            channel.QueueBind(
                queue: queueName,
                exchange: RabbitMQConstants.DirectExchangeName,
                routingKey: routingKey);
        }

        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.Received += async (model, ea) =>
        {
            var body = ea.Body.ToArray();
            var message = System.Text.Encoding.UTF8.GetString(body);
            var messageId = ea.BasicProperties?.MessageId ?? "N/A";
            var retryCount = RabbitMQConstants.GetRetryCount(ea.BasicProperties);
            
            Console.WriteLine($"\n[{consumerName}] 路由模式 - 收到消息:");
            Console.WriteLine($"  MessageId: {messageId}");
            Console.WriteLine($"  RetryCount: {retryCount}");
            Console.WriteLine($"  Queue: {queueName}");
            Console.WriteLine($"  RoutingKey: {ea.RoutingKey}");
            Console.WriteLine($"  Content: {message}");

            try
            {
                ProcessMessage(message);

                try
                {
                    channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    Console.WriteLine($"[{consumerName}] 路由模式 - 消息确认成功 (MessageId: {messageId})");
                }
                catch (Exception ackEx)
                {
                    Console.WriteLine($"[{consumerName}] 路由模式 - 消息确认失败: {ackEx.Message}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{consumerName}] 路由模式 - 消息处理失败: {ex.Message}");
                
                try
                {
                    var shouldRequeue = RabbitMQConstants.ShouldRequeue(ea.BasicProperties);
                    var newRetryCount = retryCount + 1;
                    
                    if (shouldRequeue)
                    {
                        var nextDelay = RabbitMQConstants.GetNextRetryDelay(ea.BasicProperties);
                        Console.WriteLine($"[{consumerName}] 路由模式 - 消息将重试 ({newRetryCount}/{RabbitMQConstants.MaxRetryCount})，延迟: {nextDelay}ms");
                        
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
                            routingKey: retryQueueName,
                            basicProperties: retryProperties,
                            body: body);
                        
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                        Console.WriteLine($"[{consumerName}] 路由模式 - 消息已转发到重试队列，原消息已确认");
                    }
                    else
                    {
                        Console.WriteLine($"[{consumerName}] 路由模式 - 消息已达到最大重试次数，将发送到死信队列");
                        channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                        Console.WriteLine($"[{consumerName}] 路由模式 - 消息已Nack，将进入死信队列");
                    }
                }
                catch (Exception nackEx)
                {
                    Console.WriteLine($"[{consumerName}] 路由模式 - Nack操作失败: {nackEx.Message}");
                }
            }
        };

        channel.BasicConsume(
            queue: queueName,
            autoAck: false,
            consumer: consumer);

        Console.WriteLine($"\n[{consumerName}] 路由模式消费者已启动，等待消息...");
        Console.WriteLine($"[{consumerName}] 监听队列: {queueName}");
        Console.WriteLine($"[{consumerName}] 绑定路由键: {string.Join(", ", routingKeys)}");
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