using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Shared.Services;
using RabbitMQ.Shared.Constants;
using System.Text;

namespace RabbitMQ.Consumer.Consumers;

/// <summary>
/// 死信队列消费者（Dead Letter Queue Consumer）
/// 
/// 死信队列说明：
/// - 死信队列用于存储处理失败且已达到最大重试次数的消息
/// - 消息进入死信队列的原因：
///   1. 消息被拒绝（BasicNack/BasicReject）且 requeue=false
///   2. 消息达到最大重试次数
///   3. 消息过期（TTL）
///   4. 队列达到最大长度
/// 
/// 死信消息分析：
/// - 原始消息内容
/// - 重试次数（x-retry-count）
/// - 原始队列名称（x-death.header.queue）
/// - 原始交换器名称（x-death.header.exchange）
/// - 路由键（x-death.header.routing-keys）
/// - 死信原因（x-death.header.reason）
/// 
/// 处理选项：
/// - 查看消息详情
/// - 重新投递到原队列
/// - 删除消息（手动确认）
/// - 批量处理
/// </summary>
public class DeadLetterConsumer
{
    /// <summary>
    /// 启动死信队列消费者
    /// </summary>
    /// <param name="consumerName">消费者名称</param>
    /// <param name="connectionPool">RabbitMQ连接池实例（从依赖注入获取）</param>
    public static void StartConsumer(string consumerName, RabbitMQConnectionPool? connectionPool = null)
    {
        connectionPool ??= new RabbitMQConnectionPool();
        var channel = connectionPool.GetChannel();

        // 声明死信交换器和队列（幂等操作）
        channel.ExchangeDeclare(
            exchange: RabbitMQConstants.DeadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        channel.QueueDeclare(
            queue: RabbitMQConstants.DeadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        channel.QueueBind(
            queue: RabbitMQConstants.DeadLetterQueueName,
            exchange: RabbitMQConstants.DeadLetterExchangeName,
            routingKey: RabbitMQConstants.DeadLetterQueueName);

        // QueueDeclarePassive：被动声明队列（仅查询，不创建）
        // 操作说明：
        //   - 查询队列是否存在，如果不存在会抛出异常
        //   - 返回队列信息（消息数、消费者数等）
        // 与QueueDeclare的区别：
        //   - QueueDeclare：不存在时创建队列（幂等操作）
        //   - QueueDeclarePassive：不存在时抛出异常（仅查询）
        // 使用场景：
        //   - 获取队列统计信息（消息数、消费者数）
        //   - 验证队列是否已创建
        var queueInfo = channel.QueueDeclarePassive(RabbitMQConstants.DeadLetterQueueName);
        Console.WriteLine($"\n[{consumerName}] 死信队列当前消息数: {queueInfo.MessageCount}");

        Console.WriteLine($"\n[{consumerName}] 死信队列消费者已启动");
        Console.WriteLine($"[{consumerName}] 监听队列: {RabbitMQConstants.DeadLetterQueueName}");
        Console.WriteLine($"[{consumerName}] 当前消息数: {queueInfo.MessageCount}");
        Console.WriteLine($"[{consumerName}] 手动确认模式: 开启");

        // 使用 BasicGet（手动获取模式）代替事件驱动模式
        // 原因：Console.ReadLine() 在后台线程中无法正常工作
        // BasicGet 是同步操作，在主线程中执行，可以正常获取用户输入
        while (true)
        {
            // BasicGet：手动获取一条消息
            // 参数说明：
            //   - queue: 队列名称
            //   - autoAck: 是否自动确认（false表示手动确认）
            // 返回值：GetResult 对象，包含消息数据和属性；如果队列为空则返回null
            var result = channel.BasicGet(queue: RabbitMQConstants.DeadLetterQueueName, autoAck: false);
            
            if (result == null)
            {
                Console.WriteLine($"\n[{consumerName}] 死信队列已空");
                Console.WriteLine($"[{consumerName}] 按 Enter 键继续监听，或输入 'exit' 退出: ");
                var input = Console.ReadLine();
                if (input?.Equals("exit", StringComparison.OrdinalIgnoreCase) ?? false)
                {
                    break;
                }
                continue;
            }

            try
            {
                var body = result.Body.ToArray();
                var message = System.Text.Encoding.UTF8.GetString(body);
                var messageId = result.BasicProperties?.MessageId ?? "N/A";
                var retryCount = RabbitMQConstants.GetRetryCount(result.BasicProperties);

                // 解析死信信息
                var deathInfo = ParseDeathInfo(result.BasicProperties);

                Console.WriteLine($"\n══════════════════════════════════════════════════════════════════");
                Console.WriteLine($"[{consumerName}] 死信消息详情:");
                Console.WriteLine($"──────────────────────────────────────────────────────────────────");
                Console.WriteLine($"  MessageId: {messageId}");
                Console.WriteLine($"  RetryCount: {retryCount} / {RabbitMQConstants.MaxRetryCount}");
                Console.WriteLine($"──────────────────────────────────────────────────────────────────");
                Console.WriteLine($"  死信原因: {deathInfo.Reason ?? "未知"}");
                Console.WriteLine($"  原始队列: {deathInfo.Queue ?? "未知"}");
                Console.WriteLine($"  原始交换器: {deathInfo.Exchange ?? "未知"}");
                Console.WriteLine($"  路由键: {string.Join(", ", deathInfo.RoutingKeys ?? Array.Empty<string>())}");
                Console.WriteLine($"  死信时间: {deathInfo.Time ?? DateTime.Now}");
                Console.WriteLine($"──────────────────────────────────────────────────────────────────");
                Console.WriteLine($"  消息内容: {message}");
                Console.WriteLine($"══════════════════════════════════════════════════════════════════");

                // 提供处理选项
                Console.WriteLine("\n请选择处理方式:");
                Console.WriteLine("1. 重新投递到原队列");
                Console.WriteLine("2. 删除此消息（确认）");
                Console.WriteLine("3. 跳过此消息（放回队列，继续查看下一条）");
                Console.Write("输入选项: ");

                var choice = Console.ReadLine();
                switch (choice)
                {
                    case "1":
                        // 重新投递到原队列
                        // 流程：
                        //   1. 创建新消息属性，清除死信信息和重试计数
                        //   2. 发送到默认交换器，使用原队列名称作为路由键
                        //   3. 确认死信消息（从死信队列删除）
                        RedeliverToOriginalQueue(channel, result, deathInfo);
                        Console.WriteLine($"[{consumerName}] 消息已重新投递到原队列: {deathInfo.Queue}");
                        break;
                    case "2":
                        // 删除消息（确认）
                        // BasicAck：确认消息已处理，RabbitMQ会从队列中删除该消息
                        channel.BasicAck(result.DeliveryTag, multiple: false);
                        Console.WriteLine($"[{consumerName}] 消息已删除");
                        break;
                    case "3":
                    default:
                        // 跳过（将消息放回队列头部，继续处理下一条）
                        // BasicNack(requeue: true)：拒绝消息并重新入队
                        // 注意：不能不确认消息，否则消息会一直处于Unacked状态，阻塞后续消息处理
                        channel.BasicNack(result.DeliveryTag, multiple: false, requeue: true);
                        Console.WriteLine($"[{consumerName}] 已跳过此消息，消息已放回队列");
                        break;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[{consumerName}] 处理死信消息时发生错误: {ex.Message}");
                // 发生错误时跳过，避免消费者崩溃
                // 将消息放回队列头部
                if (result != null)
                {
                    channel.BasicNack(result.DeliveryTag, multiple: false, requeue: true);
                }
            }
        }

        connectionPool.ReturnChannel(channel);
        Console.WriteLine($"\n[{consumerName}] 死信队列消费者已退出");
    }

    /// <summary>
    /// 解析死信信息
    /// 
    /// RabbitMQ死信消息会包含 x-death 头，格式为数组，包含以下信息：
    /// - reason: 死信原因（rejected/expired/maxlen）
    /// - queue: 原始队列名称
    /// - exchange: 原始交换器名称
    /// - routing-keys: 路由键数组
    /// - time: 死信时间
    /// - count: 死信次数
    /// </summary>
    /// <param name="properties">消息属性</param>
    /// <returns>死信信息对象</returns>
    private static DeathInfo ParseDeathInfo(IBasicProperties? properties)
    {
        var info = new DeathInfo();

        if (properties?.Headers != null && properties.Headers.TryGetValue("x-death", out var deathValue))
        {
            try
            {
                // x-death 通常是一个数组
                if (deathValue is List<object> deathList && deathList.Count > 0)
                {
                    var firstDeath = deathList[0] as IDictionary<string, object>;
                    if (firstDeath != null)
                    {
                        info.Reason = firstDeath.TryGetValue("reason", out var reason) ? ConvertToString(reason) : null;
                        info.Queue = firstDeath.TryGetValue("queue", out var queue) ? ConvertToString(queue) : null;
                        info.Exchange = firstDeath.TryGetValue("exchange", out var exchange) ? ConvertToString(exchange) : null;
                        
                        if (firstDeath.TryGetValue("routing-keys", out var routingKeys))
                        {
                            if (routingKeys is List<object> keys)
                            {
                                info.RoutingKeys = keys.Select(k => k?.ToString()).Where(k => !string.IsNullOrEmpty(k)).ToArray();
                            }
                        }

                        if (firstDeath.TryGetValue("time", out var time))
                        {
                            if (time is DateTime dt)
                            {
                                info.Time = dt;
                            }
                            else if (time is long timestamp)
                            {
                                // Unix时间戳（毫秒）
                                info.Time = DateTimeOffset.FromUnixTimeMilliseconds(timestamp).LocalDateTime;
                            }
                        }

                        if (firstDeath.TryGetValue("count", out var count))
                        {
                            if (count is int c)
                            {
                                info.Count = c;
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"解析死信信息失败: {ex.Message}");
            }
        }

        return info;
    }

    /// <summary>
    /// 重新投递消息到原队列
    /// 
    /// 重新投递流程：
    /// 1. 重置重试计数
    /// 2. 清除死信信息（x-death头）
    /// 3. 发布消息到原交换器
    /// 4. 确认死信消息（从死信队列删除）
    /// </summary>
    /// <param name="channel">通道实例</param>
    /// <param name="result">BasicGet返回的消息结果</param>
    /// <param name="deathInfo">死信信息</param>
    private static void RedeliverToOriginalQueue(IModel channel, BasicGetResult result, DeathInfo deathInfo)
    {
        try
        {
            // 创建新的消息属性
            var newProperties = channel.CreateBasicProperties();
            
            // 复制原始属性
            if (result.BasicProperties != null)
            {
                newProperties.Persistent = result.BasicProperties.Persistent;
                newProperties.MessageId = result.BasicProperties.MessageId;
                newProperties.ContentType = result.BasicProperties.ContentType;
                newProperties.ContentEncoding = result.BasicProperties.ContentEncoding;
                
                // 复制Headers但清除死信相关信息
                if (result.BasicProperties.Headers != null)
                {
                    newProperties.Headers = new Dictionary<string, object>(result.BasicProperties.Headers);
                    newProperties.Headers.Remove("x-death");
                    newProperties.Headers.Remove(RabbitMQConstants.RetryCountHeader);
                }
            }
            else
            {
                newProperties.Persistent = true;
            }

            // 确定目标交换器和路由键
            // 方案：使用死信交换器重新投递
            // 原因：业务队列已绑定到死信交换器（routingKey = 队列名称）
            // 这样可以确保消息通过死信交换器路由到正确的业务队列
            var targetExchange = RabbitMQConstants.DeadLetterExchangeName;
            var targetRoutingKey = deathInfo.Queue ?? string.Empty;

            // 调试日志：输出关键信息
            Console.WriteLine($"重新投递调试信息:");
            Console.WriteLine($"  - deathInfo.Queue: {deathInfo.Queue ?? "null"}");
            Console.WriteLine($"  - deathInfo.Exchange: {deathInfo.Exchange ?? "null"}");
            Console.WriteLine($"  - deathInfo.RoutingKeys: {string.Join(", ", deathInfo.RoutingKeys ?? Array.Empty<string>())}");
            Console.WriteLine($"  - targetExchange: '{targetExchange}'");
            Console.WriteLine($"  - targetRoutingKey: '{targetRoutingKey}'");

            if (string.IsNullOrEmpty(targetRoutingKey))
            {
                Console.WriteLine($"重新投递失败: 无法确定目标队列（deathInfo.Queue为空）");
                // 将消息放回死信队列，不确认
                channel.BasicNack(result.DeliveryTag, multiple: false, requeue: true);
                return;
            }

            // 验证目标队列是否存在
            try
            {
                // QueueDeclarePassive：验证队列是否存在，不存在会抛出异常
                var queueInfo = channel.QueueDeclarePassive(targetRoutingKey);
                Console.WriteLine($"目标队列 '{targetRoutingKey}' 存在，当前消息数: {queueInfo.MessageCount}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"重新投递失败: 目标队列 '{targetRoutingKey}' 不存在 - {ex.Message}");
                // 将消息放回死信队列，不确认
                channel.BasicNack(result.DeliveryTag, multiple: false, requeue: true);
                return;
            }

            // BasicPublish：重新发布消息到原队列
            // 参数说明：
            //   - exchange: 死信交换器（业务队列已绑定到此交换器）
            //   - routingKey: 原队列名称（与绑定的路由键一致）
            //   - basicProperties: 清理后的消息属性（移除x-death和x-retry-count头）
            //   - body: 原消息体
            // 操作效果：
            //   - 消息通过死信交换器路由到业务队列
            //   - 重试次数和死亡信息被重置，允许重新处理
            channel.BasicPublish(
                exchange: targetExchange,
                routingKey: targetRoutingKey,
                basicProperties: newProperties,
                body: result.Body);

            // BasicAck：确认死信消息（从死信队列删除）
            // 重新投递成功后，必须确认死信消息，否则消息会一直存在于死信队列中
            channel.BasicAck(result.DeliveryTag, multiple: false);

            Console.WriteLine($"重新投递成功: Exchange='{targetExchange}', RoutingKey='{targetRoutingKey}'");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"重新投递失败: {ex.Message}");
            // 重新投递失败时不确认消息，消息保留在死信队列中
        }
    }

    /// <summary>
    /// 将对象转换为字符串，处理字节数组类型
    /// 
    /// RabbitMQ的x-death头中，字符串字段（如queue、exchange、reason）在某些版本中存储为字节数组
    /// 直接调用ToString()会返回"System.Byte[]"而不是实际值
    /// </summary>
    /// <param name="value">要转换的对象</param>
    /// <returns>转换后的字符串，如果为null则返回null</returns>
    private static string? ConvertToString(object? value)
    {
        if (value == null)
        {
            return null;
        }

        // 如果是字节数组，解码为UTF-8字符串
        if (value is byte[] bytes)
        {
            return Encoding.UTF8.GetString(bytes);
        }

        // 其他类型直接调用ToString()
        return value.ToString();
    }

    /// <summary>
    /// 死信信息数据结构
    /// </summary>
    private class DeathInfo
    {
        /// <summary>
        /// 死信原因（rejected/expired/maxlen）
        /// </summary>
        public string? Reason { get; set; }

        /// <summary>
        /// 原始队列名称
        /// </summary>
        public string? Queue { get; set; }

        /// <summary>
        /// 原始交换器名称
        /// </summary>
        public string? Exchange { get; set; }

        /// <summary>
        /// 路由键数组
        /// </summary>
        public string?[]? RoutingKeys { get; set; }

        /// <summary>
        /// 死信时间
        /// </summary>
        public DateTime? Time { get; set; }

        /// <summary>
        /// 死信次数
        /// </summary>
        public int Count { get; set; }
    }
}