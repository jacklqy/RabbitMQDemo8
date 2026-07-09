using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Shared.Services;
using RabbitMQ.Shared.Constants;
using System.Text;

namespace RabbitMQ.Consumer.Consumers;

/// <summary>
/// 简单模式（Simple Mode）消费者
/// 
/// 模式原理：
/// - 一个生产者直接发送消息到一个队列
/// - 一个消费者从队列接收消息
/// - 使用默认交换器（空字符串）
/// - 路由键等于队列名称
/// 
/// 适用场景：一对一消息传递，简单的任务分发
/// 
/// 消息处理流程：
/// 1. 从队列接收消息
/// 2. 解析消息内容和属性（MessageId、重试次数等）
/// 3. 执行业务处理（ProcessMessage）
/// 4. 业务成功：调用BasicAck确认消息
/// 5. 业务失败：调用BasicNack拒绝消息，根据重试策略决定是否重新入队
/// 
/// 重试策略：
/// - 通过x-retry-count消息头追踪重试次数
/// - 超过MaxRetryCount后，消息发送到死信队列
/// - 使用指数退避延迟（1秒、2秒、4秒...）
/// </summary>
public class SimpleModeConsumer
{
    /// <summary>
    /// 启动简单模式消费者
    /// 
    /// 启动流程：
    /// 1. 从连接池获取通道
    /// 2. 声明队列（配置死信交换器）
    /// 3. 创建异步消费者（AsyncEventingBasicConsumer）
    /// 4. 订阅消息接收事件
    /// 5. 开始消费消息（autoAck: false，手动确认模式）
    /// 6. 等待用户输入退出
    /// 7. 归还通道到连接池
    /// </summary>
    /// <param name="consumerName">消费者名称，用于日志标识</param>
    /// <param name="connectionPool">RabbitMQ连接池实例（从依赖注入获取）</param>
    public static void StartConsumer(string consumerName, RabbitMQConnectionPool connectionPool)
    {
        // 从连接池获取通道
        var channel = connectionPool.GetChannel();

        // 声明队列（幂等操作）
        // 参数说明：
        // - queue: 队列名称
        // - durable: true - 队列持久化，RabbitMQ重启后队列保留
        // - exclusive: false - 非排他，多个消费者可同时访问
        // - autoDelete: false - 不自动删除，消费者断开后队列保留
        // - arguments: 队列参数
        //   - x-dead-letter-exchange: 死信交换器名称
        //   - x-dead-letter-routing-key: 死信路由键（死信队列名称，超过重试次数后进入死信队列）
        channel.QueueDeclare(
            queue: RabbitMQConstants.SimpleQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", RabbitMQConstants.DeadLetterExchangeName },
                { "x-dead-letter-routing-key", RabbitMQConstants.DeadLetterQueueName }
            });

        // 创建异步消费者
        // AsyncEventingBasicConsumer说明：
        // - 支持async/await回调
        // - 需要配合DispatchConsumersAsync=true使用
        // - 回调在ThreadPool线程中执行，不阻塞I/O线程
        var consumer = new AsyncEventingBasicConsumer(channel);

        // 订阅消息接收事件
        // Received事件在消息到达队列时触发
        consumer.Received += async (model, ea) =>
        {
            // 获取消息属性
            // BasicProperties包含消息的元数据：MessageId、Headers、ContentType等
            var properties = ea.BasicProperties;
            var messageId = properties?.MessageId ?? "未知";
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);

            // 获取重试次数（从消息头x-retry-count读取）
            var retryCount = RabbitMQConstants.GetRetryCount(properties);

            Console.WriteLine($"\n[{consumerName}] 接收到消息:");
            Console.WriteLine($"  MessageId: {messageId}");
            Console.WriteLine($"  重试次数: {retryCount}/{RabbitMQConstants.MaxRetryCount}");
            Console.WriteLine($"  内容: {message}");

            try
            {
                // 执行业务处理
                ProcessMessage(message);

                // BasicAck：确认消息
                // 参数说明：
                // - deliveryTag: 消息序列号，由RabbitMQ分配，全局唯一
                // - multiple: 是否批量确认（false表示只确认当前消息）
                // 操作效果：
                // - 消息从队列中删除
                // - RabbitMQ认为消息已成功处理
                channel.BasicAck(ea.DeliveryTag, multiple: false);

                Console.WriteLine($"[{consumerName}] 消息确认成功 (MessageId: {messageId})");
            }
            catch (Exception ex)
            {
                // 业务处理失败，根据重试策略决定是否重新入队
                Console.WriteLine($"[{consumerName}] 消息处理失败: {ex.Message}");

                // 判断是否需要重试
                // ShouldRequeue检查当前重试次数是否小于最大重试次数
                // 如果需要重试，会自动递增x-retry-count头
                var shouldRequeue = RabbitMQConstants.ShouldRequeue(properties);

                if (shouldRequeue)
                {
                    // 需要重试，手动发送到重试队列
                    // 新的重试次数
                    var newRetryCount = retryCount + 1;
                    // 计算退避延迟（指数退避）
                    var retryDelay = RabbitMQConstants.GetNextRetryDelay(newRetryCount);

                    Console.WriteLine($"[{consumerName}] 消息将重试 ({newRetryCount}/{RabbitMQConstants.MaxRetryCount})，延迟: {retryDelay}ms");

                    // 创建新的消息属性
                    var retryProperties = channel.CreateBasicProperties();
                    retryProperties.Persistent = true;
                    retryProperties.MessageId = messageId;
                    retryProperties.Headers = new Dictionary<string, object>
                    {
                        { RabbitMQConstants.RetryCountHeader, newRetryCount }
                    };

                    // BasicPublish：手动发送到重试队列
                    // 参数说明：
                    // - exchange: 死信交换器名称
                    // - routingKey: 重试队列名称
                    // - basicProperties: 新的消息属性（包含更新后的重试次数）
                    // - body: 原消息体
                    channel.BasicPublish(
                        exchange: RabbitMQConstants.DeadLetterExchangeName,
                        routingKey: RabbitMQConstants.SimpleQueueRetryName,
                        basicProperties: retryProperties,
                        body: body);

                    // BasicAck：确认原消息（从业务队列删除）
                    channel.BasicAck(ea.DeliveryTag, multiple: false);
                }
                else
                {
                    // 超过重试次数，发送到死信队列
                    Console.WriteLine($"[{consumerName}] 消息超过最大重试次数，将发送到死信队列 (MessageId: {messageId})");

                    // BasicNack：拒绝消息，不重新入队
                    // 参数说明：
                    // - deliveryTag: 消息序列号
                    // - multiple: 是否批量拒绝
                    // - requeue: false表示不重新入队，消息会被发送到死信队列
                    // 操作效果：
                    // - 消息被发送到配置的死信交换器
                    // - 死信交换器根据x-dead-letter-routing-key路由到死信队列
                    channel.BasicNack(ea.DeliveryTag, multiple: false, requeue: false);
                }
            }
        };

        // BasicConsume：开始消费消息
        // 参数说明：
        // - queue: 队列名称
        // - autoAck: false - 手动确认模式，需要显式调用BasicAck或BasicNack
        // - consumer: 消费者实例
        // 注意事项：
        // - autoAck=true时，消息一被接收就自动确认，无法进行重试
        // - autoAck=false时，必须显式确认，否则消息会一直处于Unacked状态
        channel.BasicConsume(queue: RabbitMQConstants.SimpleQueueName, autoAck: false, consumer: consumer);

        Console.WriteLine($"[{consumerName}] 简单模式消费者已启动，按任意键退出...");
        Console.ReadKey();

        // 归还通道到连接池
        connectionPool.ReturnChannel(channel);

        Console.WriteLine($"[{consumerName}] 简单模式消费者已停止");
    }

    /// <summary>
    /// 模拟业务处理逻辑
    /// 
    /// 演示逻辑：
    /// - 如果消息内容包含"error"，抛出异常（模拟业务失败）
    /// - 否则，成功处理
    /// </summary>
    /// <param name="message">消息内容</param>
    /// <exception cref="InvalidOperationException">当消息包含"error"时抛出</exception>
    private static void ProcessMessage(string message)
    {
        // 模拟业务处理延迟
        Thread.Sleep(1000);

        // 模拟业务失败场景
        if (message.Contains("error", StringComparison.OrdinalIgnoreCase))
        {
            throw new InvalidOperationException("业务处理失败：消息包含错误标记");
        }

        Console.WriteLine($"业务处理成功: {message}");
    }
}