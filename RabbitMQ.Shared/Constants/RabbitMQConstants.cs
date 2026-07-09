using RabbitMQ.Client;

namespace RabbitMQ.Shared.Constants;

/// <summary>
/// RabbitMQ常量定义
/// 包含队列名称、交换器名称和绑定配置
/// 
/// 设计原则：
/// 1. 统一管理所有队列和交换器名称，避免硬编码
/// 2. 生产者和消费者使用相同的常量，确保配置一致
/// 3. 队列和交换器在应用启动时初始化，确保消息不丢失
/// 4. 队列配置采用持久化策略（durable: true）
/// 
/// 消息可靠性保证：
/// - 队列持久化（durable: true）：RabbitMQ重启后队列保留
/// - 交换器持久化（durable: true）：RabbitMQ重启后交换器保留
/// - 消息持久化（Persistent: true）：消息写入磁盘
/// - 发布确认：确保消息到达RabbitMQ服务器
/// - 消费确认：手动确认，业务处理成功后才删除消息
/// - 死信队列（DLX）：处理失败的消息归档
/// - 有限重试：基于TTL的重试机制，避免无限重投递
/// </summary>
public static class RabbitMQConstants
{
    #region 重试策略配置

    /// <summary>
    /// 最大重试次数
    /// 超过此次数后，消息将被发送到死信队列
    /// </summary>
    public const int MaxRetryCount = 3;

    /// <summary>
    /// 重试退避时间（毫秒）
    /// 使用指数退避策略：1秒、2秒、4秒...
    /// 第N次重试的延迟 = RetryBackoffMs * (2^(N-1))
    /// </summary>
    public const int RetryBackoffMs = 10000;

    /// <summary>
    /// 重试计数消息头名称
    /// 用于追踪消息的重试次数
    /// </summary>
    public const string RetryCountHeader = "x-retry-count";

    #endregion

    #region 死信队列常量（Dead Letter Exchange - DLX）

    /// <summary>
    /// 死信交换器名称
    /// 所有业务队列的死信消息都会路由到这个交换器
    /// </summary>
    public const string DeadLetterExchangeName = "dlx_exchange";

    /// <summary>
    /// 死信队列名称
    /// 最终处理失败的消息存储在此队列，用于后续分析和归档
    /// </summary>
    public const string DeadLetterQueueName = "dead_letter_queue";

    #endregion

    #region 简单模式常量（Simple Mode）

    /// <summary>
    /// 简单模式队列名称
    /// 
    /// 简单模式说明：
    /// - 一个生产者直接发送消息到一个队列
    /// - 一个消费者从队列接收消息
    /// - 使用默认交换器（空字符串）
    /// - 路由键等于队列名称
    /// 
    /// 适用场景：一对一消息传递，简单的任务分发
    /// </summary>
    public const string SimpleQueueName = "simple_queue";

    /// <summary>
    /// 简单模式重试队列名称
    /// 消息处理失败后，先进入重试队列，等待TTL过期后重新入队
    /// </summary>
    public const string SimpleQueueRetryName = "simple_queue_retry";

    #endregion

    #region 工作队列模式常量（Work Queues）

    /// <summary>
    /// 工作队列模式队列名称
    /// 
    /// 工作队列模式说明：
    /// - 多个消费者共同消费一个队列中的消息
    /// - 实现负载均衡（轮询分发或公平分发）
    /// - 使用默认交换器（空字符串）
    /// - 路由键等于队列名称
    /// 
    /// 负载均衡策略：
    /// - 轮询分发（默认）：消息依次分发给各个消费者
    /// - 公平分发（BasicQos）：消费者处理完当前消息后才接收下一条
    /// 
    /// 适用场景：任务分发，多个worker处理大量任务
    /// </summary>
    public const string WorkQueueName = "work_queue";

    /// <summary>
    /// 工作队列重试队列名称
    /// </summary>
    public const string WorkQueueRetryName = "work_queue_retry";

    #endregion

    #region 发布/订阅模式常量（Pub/Sub - Fanout）

    /// <summary>
    /// Fanout交换器名称
    /// 
    /// Fanout交换器说明：
    /// - 广播模式：消息发送到所有绑定的队列
    /// - 忽略路由键：routingKey参数被忽略
    /// - 每个绑定的队列都会收到相同的消息副本
    /// 
    /// 适用场景：消息广播、日志分发、事件通知
    /// </summary>
    public const string FanoutExchangeName = "fanout_exchange";

    /// <summary>
    /// Fanout模式队列名称列表
    /// 
    /// 使用固定名称队列的原因：
    /// - 防止消息丢失：消费者未启动时，消息可存储在队列中
    /// - 支持后续消费：消费者启动后可消费累积的消息
    /// - 持久化队列：RabbitMQ重启后队列保留
    /// </summary>
    public static readonly string[] FanoutQueueNames = { "fanout_queue_1", "fanout_queue_2" };

    /// <summary>
    /// Fanout模式重试队列名称列表
    /// </summary>
    public static readonly string[] FanoutRetryQueueNames = { "fanout_queue_1_retry", "fanout_queue_2_retry" };

    #endregion

    #region 路由模式常量（Routing - Direct）

    /// <summary>
    /// Direct交换器名称
    /// 
    /// Direct交换器说明：
    /// - 精确匹配：消息路由键与队列绑定键完全匹配
    /// - 一个队列可绑定多个路由键
    /// - 消息只发送到匹配的队列
    /// 
    /// 适用场景：日志分级处理（error/info/warning）、按类型分发消息
    /// </summary>
    public const string DirectExchangeName = "direct_exchange";

    /// <summary>
    /// Direct模式队列名称列表
    /// </summary>
    public static readonly string[] DirectQueueNames = 
    { 
        "direct_error_queue",   // 接收error级别的日志
        "direct_info_queue",    // 接收info级别的日志
        "direct_warning_queue", // 接收warning级别的日志
        "direct_all_queue"      // 接收所有级别的日志
    };

    /// <summary>
    /// Direct模式重试队列名称列表
    /// </summary>
    public static readonly string[] DirectRetryQueueNames = 
    { 
        "direct_error_queue_retry",
        "direct_info_queue_retry",
        "direct_warning_queue_retry",
        "direct_all_queue_retry"
    };

    /// <summary>
    /// Direct模式队列与路由键的映射配置
    /// Key: 队列名称
    /// Value: 路由键数组（一个队列可绑定多个路由键）
    /// 
    /// 配置说明：
    /// - direct_error_queue: 绑定"error"，只接收error消息
    /// - direct_info_queue: 绑定"info"，只接收info消息
    /// - direct_warning_queue: 绑定"warning"，只接收warning消息
    /// - direct_all_queue: 绑定"error"、"info"、"warning"，接收所有消息
    /// </summary>
    public static readonly Dictionary<string, string[]> DirectQueueRoutingKeyMap = new()
    {
        { "direct_error_queue", new[] { "error" } },
        { "direct_info_queue", new[] { "info" } },
        { "direct_warning_queue", new[] { "warning" } },
        { "direct_all_queue", new[] { "error", "info", "warning" } }
    };

    #endregion

    #region 主题模式常量（Topics）

    /// <summary>
    /// Topic交换器名称
    /// 
    /// Topic交换器说明：
    /// - 通配符匹配：支持*和#通配符
    /// - * (星号)：匹配一个词
    /// - # (井号)：匹配零个或多个词
    /// - 路由键用点号分隔（如 user.create, order.pay.success）
    /// 
    /// 通配符示例：
    /// - user.*：匹配 user.create, user.update, user.delete
    /// - order.#：匹配 order.create, order.pay, order.pay.success
    /// - *.create：匹配 user.create, order.create, product.create
    /// - #：匹配所有路由键
    /// 
    /// 适用场景：复杂的消息路由、事件驱动架构
    /// </summary>
    public const string TopicExchangeName = "topic_exchange";

    /// <summary>
    /// Topic模式队列名称列表
    /// </summary>
    public static readonly string[] TopicQueueNames = 
    { 
        "topics_user_queue",    // 接收用户相关事件
        "topics_order_queue",   // 接收订单相关事件
        "topics_create_queue",  // 接收创建事件
        "topics_all_queue"      // 接收所有事件
    };

    /// <summary>
    /// Topic模式重试队列名称列表
    /// </summary>
    public static readonly string[] TopicRetryQueueNames = 
    { 
        "topics_user_queue_retry",
        "topics_order_queue_retry",
        "topics_create_queue_retry",
        "topics_all_queue_retry"
    };

    /// <summary>
    /// Topic模式队列与主题模式的映射配置
    /// Key: 队列名称
    /// Value: 主题模式数组（支持通配符）
    /// 
    /// 配置说明：
    /// - topics_user_queue: 绑定"user.*"，匹配所有用户事件（user.create, user.update等）
    /// - topics_order_queue: 绑定"order.#"，匹配所有订单事件（order.create, order.pay.success等）
    /// - topics_create_queue: 绑定"*.create"，匹配所有创建事件（user.create, order.create等）
    /// - topics_all_queue: 绑定"#"，匹配所有事件
    /// </summary>
    public static readonly Dictionary<string, string[]> TopicQueueBindingMap = new()
    {
        { "topics_user_queue", new[] { "user.*" } },           // 匹配所有用户相关事件
        { "topics_order_queue", new[] { "order.#" } },         // 匹配所有订单相关事件
        { "topics_create_queue", new[] { "*.create" } },       // 匹配所有创建事件
        { "topics_all_queue", new[] { "#" } }                 // 匹配所有事件
    };

    #endregion

    /// <summary>
    /// 初始化所有队列和交换器
    /// 在应用启动时调用（通过RabbitMQInitializer）
    /// 
    /// 初始化流程：
    /// 1. 初始化死信交换器和死信队列
    /// 2. 初始化简单模式队列（含重试队列）
    /// 3. 初始化工作队列模式队列（含重试队列）
    /// 4. 初始化Fanout模式队列和交换器（含重试队列）
    /// 5. 初始化Direct模式队列和交换器（含重试队列）
    /// 6. 初始化Topic模式队列和交换器（含重试队列）
    /// 
    /// 幂等性保证：
    /// - QueueDeclare和ExchangeDeclare是幂等操作
    /// - 队列或交换器已存在时，不会重新创建
    /// - 参数必须与已存在的实体一致，否则抛出异常
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    public static void InitializeAllQueues(IModel channel)
    {
        InitializeDeadLetterExchange(channel);
        InitializeSimpleQueue(channel);
        InitializeWorkQueue(channel);
        InitializeFanoutQueues(channel);
        InitializeDirectQueues(channel);
        InitializeTopicQueues(channel);
    }

    /// <summary>
    /// 初始化死信交换器和死信队列
    /// 
    /// 死信队列设计：
    /// - 死信交换器（DLX）：接收所有业务队列的死信消息
    /// - 死信队列：存储最终处理失败的消息，用于后续分析
    /// - 所有业务队列配置 x-dead-letter-exchange 参数指向此交换器
    /// 
    /// 重试机制设计：
    /// - 每个业务队列对应一个重试队列
    /// - 重试队列配置 x-message-ttl 和 x-dead-letter-exchange
    /// - TTL过期后，消息自动死信回原业务队列
    /// - 通过 x-retry-count 头追踪重试次数
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeDeadLetterExchange(IModel channel)
    {
        // ExchangeDeclare：声明交换器
        // 参数说明：
        //   - exchange: 交换器名称
        //   - type: 交换器类型（Direct/Fanout/Topic/Headers）
        //   - durable: 是否持久化（true表示RabbitMQ重启后保留）
        //   - autoDelete: 是否自动删除（true表示所有绑定解绑后自动删除）
        //   - arguments: 其他参数（如备用交换器等）
        // 注意事项：
        //   - 幂等操作：同名交换器已存在且参数一致时不做任何操作
        //   - 如果参数不一致会抛出PRECONDITION_FAILED错误
        channel.ExchangeDeclare(
            exchange: DeadLetterExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        // QueueDeclare：声明队列
        // 参数说明：
        //   - queue: 队列名称
        //   - durable: 是否持久化
        //   - exclusive: 是否排他（true表示只有当前连接可访问）
        //   - autoDelete: 是否自动删除
        //   - arguments: 队列参数（死信交换器、TTL、最大长度等）
        // 注意事项：
        //   - 幂等操作：同名队列已存在且参数一致时不做任何操作
        //   - 参数不一致会抛出PRECONDITION_FAILED错误
        channel.QueueDeclare(
            queue: DeadLetterQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: null);

        // QueueBind：绑定队列到交换器
        // 参数说明：
        //   - queue: 队列名称
        //   - exchange: 交换器名称
        //   - routingKey: 路由键（Direct/Topic模式使用，Fanout模式可忽略）
        //   - arguments: 绑定参数
        // 注意事项：
        //   - 幂等操作：相同绑定已存在时不做任何操作
        //   - 一个队列可以绑定到多个交换器
        // 将死信队列绑定到死信交换器，路由键使用队列名称
        channel.QueueBind(
            queue: DeadLetterQueueName,
            exchange: DeadLetterExchangeName,
            routingKey: DeadLetterQueueName);

        Console.WriteLine($"[RabbitMQConstants] 已初始化死信交换器和队列");
    }

    /// <summary>
    /// 获取业务队列对应的重试队列名称
    /// </summary>
    /// <param name="queueName">业务队列名称</param>
    /// <returns>重试队列名称</returns>
    public static string GetRetryQueueName(string queueName)
    {
        return $"{queueName}_retry";
    }

    /// <summary>
    /// 初始化简单模式队列（含重试队列）
    /// 
    /// 队列参数说明：
    /// - durable: true - 队列持久化，RabbitMQ重启后队列保留
    /// - exclusive: false - 非排他，多个消费者可同时访问
    /// - autoDelete: false - 不自动删除，消费者断开后队列保留
    /// - arguments: 配置死信交换器参数
    ///   - x-dead-letter-exchange: 死信交换器名称
    ///   - x-dead-letter-routing-key: 死信路由键（重试队列名称）
    /// 
    /// 重试队列参数：
    /// - x-message-ttl: 消息存活时间（毫秒），TTL过期后消息死信回原队列
    /// - x-dead-letter-exchange: 死信交换器名称
    /// - x-dead-letter-routing-key: 原队列名称（TTL过期后路由回原队列）
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeSimpleQueue(IModel channel)
    {
        // QueueDeclare：声明重试队列
        // 参数说明：
        //   - x-message-ttl: 消息存活时间（1000ms），TTL过期后消息自动死信
        //   - x-dead-letter-exchange: TTL过期后路由到死信交换器
        //   - x-dead-letter-routing-key: TTL过期后使用此路由键（回原业务队列）
        // 重试队列工作原理：
        //   1. 消息发送到重试队列后等待TTL过期
        //   2. TTL过期后消息死信到死信交换器
        //   3. 死信交换器根据路由键将消息路由回业务队列
        channel.QueueDeclare(
            queue: SimpleQueueRetryName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-message-ttl", RetryBackoffMs },
                { "x-dead-letter-exchange", DeadLetterExchangeName },
                { "x-dead-letter-routing-key", SimpleQueueName }
            });

        // QueueBind：将重试队列绑定到死信交换器
        // 路由键使用重试队列名称，消费者手动发送消息到重试队列时使用此路由键
        channel.QueueBind(
            queue: SimpleQueueRetryName,
            exchange: DeadLetterExchangeName,
            routingKey: SimpleQueueRetryName);

        // QueueDeclare：声明业务队列（配置死信交换器）
        // 参数说明：
        //   - x-dead-letter-exchange: 消息被拒绝（requeue=false）后路由到死信交换器
        //   - x-dead-letter-routing-key: 被拒绝后使用此路由键（到死信队列）
        // 业务队列工作原理：
        //   1. 正常消息从生产者发送到业务队列
        //   2. 消费者消费失败且超过重试次数时，BasicNack(requeue=false)
        //   3. 消息死信到死信交换器，路由到死信队列
        channel.QueueDeclare(
            queue: SimpleQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", DeadLetterExchangeName },
                { "x-dead-letter-routing-key", DeadLetterQueueName }
            });

        // QueueBind：将业务队列绑定到死信交换器
        // 路由键使用业务队列名称，重试队列TTL过期后消息通过此绑定回到业务队列
        channel.QueueBind(
            queue: SimpleQueueName,
            exchange: DeadLetterExchangeName,
            routingKey: SimpleQueueName);

        Console.WriteLine($"[RabbitMQConstants] 已初始化简单模式队列: {SimpleQueueName} (含重试队列: {SimpleQueueRetryName})");
    }

    /// <summary>
    /// 初始化工作队列模式队列（含重试队列）
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeWorkQueue(IModel channel)
    {
        // 声明重试队列
        channel.QueueDeclare(
            queue: WorkQueueRetryName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-message-ttl", RetryBackoffMs },
                { "x-dead-letter-exchange", DeadLetterExchangeName },
                { "x-dead-letter-routing-key", WorkQueueName }
            });

        // 将重试队列绑定到死信交换器
        channel.QueueBind(
            queue: WorkQueueRetryName,
            exchange: DeadLetterExchangeName,
            routingKey: WorkQueueRetryName);

        // 声明主队列（配置死信交换器）
        // 死信路由键设置为死信队列名称，确保超过重试次数的消息直接进入死信队列
        channel.QueueDeclare(
            queue: WorkQueueName,
            durable: true,
            exclusive: false,
            autoDelete: false,
            arguments: new Dictionary<string, object>
            {
                { "x-dead-letter-exchange", DeadLetterExchangeName },
                { "x-dead-letter-routing-key", DeadLetterQueueName }
            });

        // 将主队列绑定到死信交换器
        // 这样重试队列的消息TTL过期后，能通过死信交换器路由回主队列
        channel.QueueBind(
            queue: WorkQueueName,
            exchange: DeadLetterExchangeName,
            routingKey: WorkQueueName);

        Console.WriteLine($"[RabbitMQConstants] 已初始化工作队列模式队列: {WorkQueueName} (含重试队列: {WorkQueueRetryName})");
    }

    /// <summary>
    /// 初始化Fanout模式队列和交换器（含重试队列）
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeFanoutQueues(IModel channel)
    {
        // 声明Fanout交换器（幂等操作）
        channel.ExchangeDeclare(
            exchange: FanoutExchangeName,
            type: ExchangeType.Fanout,
            durable: true,
            autoDelete: false,
            arguments: null);

        for (int i = 0; i < FanoutQueueNames.Length; i++)
        {
            var queueName = FanoutQueueNames[i];
            var retryQueueName = FanoutRetryQueueNames[i];

            // 声明重试队列
            channel.QueueDeclare(
                queue: retryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-message-ttl", RetryBackoffMs },
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", queueName }
                });

            // 将重试队列绑定到死信交换器
            channel.QueueBind(
                queue: retryQueueName,
                exchange: DeadLetterExchangeName,
                routingKey: retryQueueName);

            // 声明主队列（配置死信交换器）
            // 死信路由键设置为死信队列名称，确保超过重试次数的消息直接进入死信队列
            channel.QueueDeclare(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", DeadLetterQueueName }
                });

            // 将主队列绑定到Fanout交换器
            channel.QueueBind(
                queue: queueName,
                exchange: FanoutExchangeName,
                routingKey: string.Empty);

            // 将主队列绑定到死信交换器
            // 这样重试队列的消息TTL过期后，能通过死信交换器路由回主队列
            channel.QueueBind(
                queue: queueName,
                exchange: DeadLetterExchangeName,
                routingKey: queueName);
        }

        Console.WriteLine($"[RabbitMQConstants] 已初始化 {FanoutQueueNames.Length} 个Fanout队列并绑定到交换器: {FanoutExchangeName}");
    }

    /// <summary>
    /// 初始化Direct模式队列和交换器（含重试队列）
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeDirectQueues(IModel channel)
    {
        // 声明Direct交换器（幂等操作）
        channel.ExchangeDeclare(
            exchange: DirectExchangeName,
            type: ExchangeType.Direct,
            durable: true,
            autoDelete: false,
            arguments: null);

        for (int i = 0; i < DirectQueueNames.Length; i++)
        {
            var queueName = DirectQueueNames[i];
            var retryQueueName = DirectRetryQueueNames[i];
            var routingKeys = DirectQueueRoutingKeyMap.TryGetValue(queueName, out var keys) ? keys : new[] { queueName };

            // 声明重试队列
            channel.QueueDeclare(
                queue: retryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-message-ttl", RetryBackoffMs },
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", queueName }
                });

            // 将重试队列绑定到死信交换器
            channel.QueueBind(
                queue: retryQueueName,
                exchange: DeadLetterExchangeName,
                routingKey: retryQueueName);

            // 声明主队列（配置死信交换器）
            // 死信路由键设置为死信队列名称，确保超过重试次数的消息直接进入死信队列
            channel.QueueDeclare(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", DeadLetterQueueName }
                });

            // 将主队列绑定到Direct交换器
            foreach (var routingKey in routingKeys)
            {
                channel.QueueBind(
                    queue: queueName,
                    exchange: DirectExchangeName,
                    routingKey: routingKey);
            }

            // 将主队列绑定到死信交换器
            // 这样重试队列的消息TTL过期后，能通过死信交换器路由回主队列
            channel.QueueBind(
                queue: queueName,
                exchange: DeadLetterExchangeName,
                routingKey: queueName);
        }

        Console.WriteLine($"[RabbitMQConstants] 已初始化 {DirectQueueRoutingKeyMap.Count} 个Direct队列并绑定到交换器: {DirectExchangeName}");
    }

    /// <summary>
    /// 初始化Topic模式队列和交换器（含重试队列）
    /// </summary>
    /// <param name="channel">RabbitMQ通道实例</param>
    private static void InitializeTopicQueues(IModel channel)
    {
        // 声明Topic交换器（幂等操作）
        channel.ExchangeDeclare(
            exchange: TopicExchangeName,
            type: ExchangeType.Topic,
            durable: true,
            autoDelete: false,
            arguments: null);

        for (int i = 0; i < TopicQueueNames.Length; i++)
        {
            var queueName = TopicQueueNames[i];
            var retryQueueName = TopicRetryQueueNames[i];
            var bindings = TopicQueueBindingMap.TryGetValue(queueName, out var b) ? b : new[] { queueName };

            // 声明重试队列
            channel.QueueDeclare(
                queue: retryQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-message-ttl", RetryBackoffMs },
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", queueName }
                });

            // 将重试队列绑定到死信交换器
            channel.QueueBind(
                queue: retryQueueName,
                exchange: DeadLetterExchangeName,
                routingKey: retryQueueName);

            // 声明主队列（配置死信交换器）
            // 死信路由键设置为死信队列名称，确保超过重试次数的消息直接进入死信队列
            channel.QueueDeclare(
                queue: queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", DeadLetterExchangeName },
                    { "x-dead-letter-routing-key", DeadLetterQueueName }
                });

            // 将主队列绑定到Topic交换器
            foreach (var binding in bindings)
            {
                channel.QueueBind(
                    queue: queueName,
                    exchange: TopicExchangeName,
                    routingKey: binding);
            }

            // 将主队列绑定到死信交换器
            // 这样重试队列的消息TTL过期后，能通过死信交换器路由回主队列
            channel.QueueBind(
                queue: queueName,
                exchange: DeadLetterExchangeName,
                routingKey: queueName);
        }

        Console.WriteLine($"[RabbitMQConstants] 已初始化 {TopicQueueBindingMap.Count} 个Topic队列并绑定到交换器: {TopicExchangeName}");
    }

    /// <summary>
    /// 根据队列名称获取Direct模式的路由键
    /// </summary>
    /// <param name="queueName">队列名称</param>
    /// <returns>路由键数组</returns>
    public static string[] GetDirectRoutingKeys(string queueName)
    {
        return DirectQueueRoutingKeyMap.TryGetValue(queueName, out var keys) ? keys : new[] { queueName };
    }

    /// <summary>
    /// 根据队列名称获取Topic模式的绑定模式
    /// </summary>
    /// <param name="queueName">队列名称</param>
    /// <returns>绑定模式数组</returns>
    public static string[] GetTopicBindings(string queueName)
    {
        return TopicQueueBindingMap.TryGetValue(queueName, out var bindings) ? bindings : new[] { queueName };
    }

    /// <summary>
    /// 判断消息是否应该重新入队（有限重试）
    /// 
    /// 重试策略：
    /// 1. 从消息头获取当前重试次数
    /// 2. 如果重试次数 < 最大重试次数，返回true（重新入队）
    /// 3. 如果重试次数 >= 最大重试次数，返回false（发送到死信队列）
    /// 
    /// 退避策略：
    /// - 使用指数退避：第N次重试的延迟 = RetryBackoffMs * (2^(N-1))
    /// - 通过修改消息的x-message-ttl实现动态退避
    /// </summary>
    /// <param name="properties">消息属性</param>
    /// <returns>true表示重新入队，false表示发送到死信队列</returns>
    public static bool ShouldRequeue(IBasicProperties properties)
    {
        // 获取当前重试次数
        var currentRetryCount = GetRetryCount(properties);

        // 判断是否需要重试
        if (currentRetryCount < MaxRetryCount)
        {
            // 更新重试次数
            IncrementRetryCount(properties);
            return true;
        }

        // 超过最大重试次数，发送到死信队列
        return false;
    }

    /// <summary>
    /// 获取消息的重试次数
    /// </summary>
    /// <param name="properties">消息属性</param>
    /// <returns>当前重试次数</returns>
    public static int GetRetryCount(IBasicProperties properties)
    {
        if (properties.Headers != null && properties.Headers.TryGetValue(RetryCountHeader, out var value))
        {
            if (value is int count)
            {
                return count;
            }
            // 处理byte[]类型（RabbitMQ有时会将int存储为byte[]）
            if (value is byte[] bytes && bytes.Length == 4)
            {
                return BitConverter.ToInt32(bytes, 0);
            }
        }
        return 0;
    }

    /// <summary>
    /// 增加消息的重试次数
    /// </summary>
    /// <param name="properties">消息属性</param>
    public static void IncrementRetryCount(IBasicProperties properties)
    {
        var currentCount = GetRetryCount(properties);
        
        if (properties.Headers == null)
        {
            properties.Headers = new Dictionary<string, object>();
        }
        
        properties.Headers[RetryCountHeader] = currentCount + 1;
    }

    /// <summary>
    /// 获取下一次重试的延迟时间（毫秒）
    /// 使用指数退避策略
    /// </summary>
    /// <param name="properties">消息属性</param>
    /// <returns>下一次重试的延迟时间（毫秒）</returns>
    public static int GetNextRetryDelay(IBasicProperties properties)
    {
        var retryCount = GetRetryCount(properties);
        return GetNextRetryDelay(retryCount);
    }

    /// <summary>
    /// 获取下一次重试的延迟时间（毫秒）
    /// 使用指数退避策略
    /// </summary>
    /// <param name="retryCount">当前重试次数</param>
    /// <returns>下一次重试的延迟时间（毫秒）</returns>
    public static int GetNextRetryDelay(int retryCount)
    {
        // 指数退避：1秒、2秒、4秒...
        return RetryBackoffMs * (int)Math.Pow(2, retryCount);
    }
}