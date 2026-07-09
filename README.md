# RabbitMQ Demo (.NET 8)

一个基于 .NET 8 的 RabbitMQ 消息队列完整演示项目，包含生产者（WebApi）和消费者（控制台客户端），支持五种核心模式，并实现了完整的消息可靠性保证机制。

## 功能特性

- ✅ **简单模式 (Simple Mode)**：一对一消息传递
- ✅ **工作队列模式 (Work Queues)**：多消费者负载均衡
- ✅ **发布/订阅模式 (Pub/Sub - Fanout)**：消息广播
- ✅ **路由模式 (Routing - Direct)**：精确路由匹配
- ✅ **主题模式 (Topics)**：通配符路由匹配
- ✅ **发布确认 (Producer Confirms)**：确保消息到达RabbitMQ
- ✅ **消费确认 (Consumer Acknowledgements)**：手动确认，防止消息丢失
- ✅ **消息持久化**：队列、交换器、消息均持久化
- ✅ **固定队列设计**：Fanout/Direct/Topics模式使用固定队列，防止消息丢失
- ✅ **异步消费者**：使用 AsyncEventingBasicConsumer 支持 async/await
- ✅ **死信队列 (DLX)**：处理失败的消息归档和分析
- ✅ **有限重试+退避策略**：指数退避，避免无限重投递
- ✅ **连接池**：复用连接和通道资源
- ✅ **配置文件读取**：RabbitMQ连接参数从配置文件读取，支持环境变量覆盖
- ✅ **结构化日志**：使用ILogger记录日志，便于问题排查

## 技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| .NET | 8.0 | 运行时框架 |
| RabbitMQ.Client | 6.8.1 | RabbitMQ客户端库 |
| ASP.NET Core | 8.0 | WebApi框架 |
| Swashbuckle.AspNetCore | 6.5.0 | Swagger文档生成 |
| Microsoft.Extensions.Options | 8.0.0 | 配置选项管理 |

## 项目结构

```
RabbitMQDemo8/
├── RabbitMQDemo.sln                    # 解决方案文件
├── RabbitMQ.Shared/                    # 共享类库（核心服务）
│   ├── Services/
│   │   ├── RabbitMQConnectionFactory.cs   # 连接工厂（支持依赖注入）
│   │   ├── RabbitMQService.cs             # RabbitMQ核心服务（含发布确认）
│   │   └── RabbitMQConnectionPool.cs      # 连接池（复用连接和通道）
│   ├── Constants/
│   │   └── RabbitMQConstants.cs           # 队列/交换器常量配置
│   ├── Options/
│   │   └── RabbitMQOptions.cs             # RabbitMQ配置选项类
│   ├── HostedServices/
│   │   └── RabbitMQInitializer.cs         # 应用启动时初始化队列
│   └── Models/
│       ├── MessageRequest.cs              # 消息请求模型
│       ├── RoutingMessageRequest.cs       # 路由模式请求模型
│       └── TopicMessageRequest.cs         # 主题模式请求模型
├── RabbitMQ.Producer/                  # 生产者（WebApi）
│   ├── Controllers/
│   │   ├── SimpleModeController.cs        # 简单模式控制器
│   │   ├── WorkQueuesController.cs        # 工作队列控制器
│   │   ├── PubSubController.cs            # 发布/订阅控制器
│   │   ├── RoutingController.cs           # 路由模式控制器
│   │   └── TopicsController.cs            # 主题模式控制器
│   ├── Program.cs                         # 启动配置（依赖注入注册）
│   └── appsettings.json                   # 应用配置（含RabbitMQ配置）
└── RabbitMQ.Consumer/                  # 消费者（控制台客户端）
    ├── Consumers/
    │   ├── SimpleModeConsumer.cs          # 简单模式消费者
    │   ├── WorkQueuesConsumer.cs          # 工作队列消费者
    │   ├── PubSubConsumer.cs              # 发布/订阅消费者
    │   ├── RoutingConsumer.cs             # 路由模式消费者
    │   ├── TopicsConsumer.cs              # 主题模式消费者
    │   └── DeadLetterConsumer.cs          # 死信队列消费者
    ├── Program.cs                         # 消费者入口（配置读取和DI）
    └── appsettings.json                   # 消费者配置（含RabbitMQ配置）
```

## 配置说明

### 配置文件结构

RabbitMQ 连接参数通过 `appsettings.json` 配置，支持环境变量覆盖。

**生产者配置** (`RabbitMQ.Producer/appsettings.json`)：

```json
{
  "RabbitMQ": {
    "HostName": "localhost",
    "UserName": "guest",
    "Password": "guest",
    "Port": 5672,
    "VirtualHost": "/",
    "ConnectionTimeout": 30000,
    "RequestedHeartbeat": 60,
    "MaxChannels": 10,
    "MaxRetryCount": 3,
    "RetryBackoffMs": 1000
  }
}
```

**消费者配置** (`RabbitMQ.Consumer/appsettings.json`)：

```json
{
  "RabbitMQ": {
    "HostName": "localhost",
    "UserName": "guest",
    "Password": "guest",
    "Port": 5672,
    "VirtualHost": "/",
    "ConnectionTimeout": 30000,
    "RequestedHeartbeat": 60,
    "MaxChannels": 10,
    "MaxRetryCount": 3,
    "RetryBackoffMs": 1000
  }
}
```

### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| HostName | string | localhost | RabbitMQ服务器地址 |
| UserName | string | guest | 用户名 |
| Password | string | guest | 密码 |
| Port | int | 5672 | 端口号 |
| VirtualHost | string | / | 虚拟主机 |
| ConnectionTimeout | int | 30000 | 连接超时时间（毫秒） |
| RequestedHeartbeat | ushort | 60 | 心跳超时时间（秒） |
| MaxChannels | int | 10 | 连接池最大通道数 |
| MaxRetryCount | int | 3 | 最大重试次数 |
| RetryBackoffMs | int | 1000 | 重试退避基础延迟（毫秒） |

### 环境变量覆盖

生产环境中可通过环境变量覆盖配置：

```bash
# 设置RabbitMQ主机
set RabbitMQ__HostName=production-rabbitmq.example.com

# 设置用户名和密码
set RabbitMQ__UserName=prod-user
set RabbitMQ__Password=prod-password
```

### 依赖注入注册

**生产者** (`RabbitMQ.Producer/Program.cs`)：

```csharp
// 绑定RabbitMQ配置（从appsettings.json读取）
builder.Services.Configure<RabbitMQOptions>(builder.Configuration.GetSection(RabbitMQOptions.SectionName));

// 注册RabbitMQ服务（按依赖顺序注册）
builder.Services.AddSingleton<RabbitMQConnectionFactory>();
builder.Services.AddSingleton<RabbitMQService>();
builder.Services.AddSingleton<RabbitMQConnectionPool>();

// 注册初始化托管服务（应用启动时初始化队列和交换器）
builder.Services.AddHostedService<RabbitMQInitializer>();
```

**消费者** (`RabbitMQ.Consumer/Program.cs`)：

```csharp
var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

var services = new ServiceCollection();
services.Configure<RabbitMQOptions>(configuration.GetSection(RabbitMQOptions.SectionName));
services.AddLogging(builder => builder.AddConsole());
services.AddSingleton<RabbitMQConnectionFactory>();
services.AddSingleton<RabbitMQConnectionPool>();
var serviceProvider = services.BuildServiceProvider();
```

## 核心概念

### 1. 发布确认机制

发布确认确保消息已成功到达RabbitMQ服务器，防止消息在网络传输中丢失。

**实现方式**：
- 调用 `channel.ConfirmSelect()` 启用发布确认模式
- 使用 `TaskCompletionSource` 追踪每条消息的确认状态
- 监听 `BasicAcks` 和 `BasicNacks` 事件处理确认结果
- 设置超时机制，超时未确认则抛出异常

```csharp
// 核心流程（RabbitMQService.cs）
var tcs = new TaskCompletionSource<bool>();
var deliveryTag = _channel.NextPublishSeqNo;
_pendingConfirms.TryAdd(deliveryTag, tcs);

_channel.BasicPublish(exchange, routingKey, properties, body);
var confirmed = await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
if (!confirmed) throw new InvalidOperationException("消息发布未被确认");
```

### 2. 消费确认机制

消费确认确保业务处理成功后才删除消息，防止处理过程中消息丢失。

**实现方式**：
- 设置 `autoAck: false` 启用手动确认模式
- 业务处理成功后调用 `BasicAck` 确认消息
- 业务处理失败时调用 `BasicNack` 拒绝消息
- 根据异常类型决定是否重新入队（`requeue` 参数）

```csharp
// 手动确认（消费者代码）
channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);

// 拒绝消息（消费者代码）
channel.BasicNack(deliveryTag: ea.DeliveryTag, multiple: false, requeue: ShouldRequeue(ex));
```

### 3. 有限重试+退避策略

对可重试异常实现有限次数重试，并使用指数退避策略避免消息风暴。

**实现方式**：
- 通过消息头 `x-retry-count` 追踪重试次数
- 使用指数退避算法计算重试延迟（1秒、2秒、4秒...）
- 超过最大重试次数后，消息进入死信队列

**重试延迟计算**：
```csharp
// 指数退避：RetryBackoffMs * 2^retryCount
// 示例：1000ms * 2^0 = 1000ms（第1次重试）
//      1000ms * 2^1 = 2000ms（第2次重试）
//      1000ms * 2^2 = 4000ms（第3次重试）
var retryDelay = RabbitMQConstants.RetryBackoffMs * (int)Math.Pow(2, retryCount);
```

**重试流程**：
```
业务队列 → 处理失败 → 检查重试次数
    ↓                    ↓
 重试队列 ← 发送重试消息 ← 未达上限
    ↓
 延迟消费（退避等待）
    ↓
 重新投递到业务队列
    ↓
 达到最大重试次数 → 死信队列
```

### 4. 死信队列 (DLX)

死信队列用于存储处理失败且达到最大重试次数的消息，支持消息分析和手动处理。

**死信队列配置**：
- 通过 `x-dead-letter-exchange` 设置死信交换器
- 通过 `x-dead-letter-routing-key` 设置死信路由键
- 死信队列绑定到死信交换器

**死信队列消费者操作**：
- **重新投递**：将消息重新发送到原队列
- **删除**：确认消息，从死信队列移除
- **跳过**：不确认消息，保留在死信队列中

### 5. 连接池

连接池用于复用RabbitMQ连接和通道资源，减少连接创建开销。

**核心特性**：
- 单连接多通道模式，减少TCP连接数
- 通道复用，避免频繁创建和销毁通道
- 线程安全的通道获取和释放

### 6. 消息持久化

确保RabbitMQ重启后消息不丢失：

| 持久化级别 | 设置方式 | 说明 |
|-----------|---------|------|
| 队列持久化 | `durable: true` | 队列在RabbitMQ重启后保留 |
| 交换器持久化 | `durable: true` | 交换器在RabbitMQ重启后保留 |
| 消息持久化 | `Persistent = true` | 消息写入磁盘 |

### 7. 公平分发（BasicQos）

工作队列模式中启用公平分发，防止负载不均：

```csharp
// 每次只接收1条消息，处理完再接收下一条
channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
```

## 五种模式详细说明

### 1. 简单模式 (Simple Mode)

**原理**：一个生产者直接发送消息到一个队列，一个消费者从队列接收消息。

**架构**：
```
生产者 → [默认交换器] → [simple_queue] → 消费者
```

**适用场景**：一对一消息传递，简单的任务处理。

**API接口**：`POST /api/SimpleMode`

### 2. 工作队列模式 (Work Queues)

**原理**：多个消费者共同消费一个队列中的消息，实现负载均衡。

**架构**：
```
生产者 → [默认交换器] → [work_queue] → 消费者1
                                   → 消费者2
                                   → 消费者3
```

**负载均衡策略**：
- **轮询分发（默认）**：消息依次分发给各个消费者
- **公平分发（BasicQos）**：消费者处理完当前消息后才接收下一条

**适用场景**：任务分发，多个worker处理大量任务。

**API接口**：`POST /api/WorkQueues`

### 3. 发布/订阅模式 (Pub/Sub - Fanout)

**原理**：Fanout交换器将消息广播到所有绑定的队列，忽略路由键。

**架构**：
```
生产者 → [fanout_exchange] → [fanout_queue_1] → 消费者1
                          → [fanout_queue_2] → 消费者2
```

**特性**：
- 每个绑定的队列都会收到相同的消息副本
- 使用固定队列名称，消费者未启动时消息可存储

**适用场景**：消息广播、日志分发、事件通知。

**API接口**：`POST /api/PubSub`

### 4. 路由模式 (Routing - Direct)

**原理**：Direct交换器根据路由键精确匹配，消息只发送到匹配的队列。

**架构**：
```
生产者 → [direct_exchange] → [direct_error_queue]  (绑定: error)
                          → [direct_info_queue]   (绑定: info)
                          → [direct_all_queue]    (绑定: error, info, warning)
```

**队列配置**：

| 队列名称 | 绑定路由键 | 说明 |
|----------|-----------|------|
| `direct_error_queue` | error | 只接收error消息 |
| `direct_info_queue` | info | 只接收info消息 |
| `direct_warning_queue` | warning | 只接收warning消息 |
| `direct_all_queue` | error, info, warning | 接收所有消息 |

**适用场景**：日志分级处理、按类型分发消息。

**API接口**：`POST /api/Routing`

### 5. 主题模式 (Topics)

**原理**：Topic交换器支持通配符匹配路由键。

**通配符规则**：
- `*`（星号）：匹配一个词
- `#`（井号）：匹配零个或多个词
- 路由键用点号分隔（如 `user.create`, `order.pay.success`）

**架构**：
```
生产者 → [topic_exchange] → [topics_user_queue]   (绑定: user.*)
                          → [topics_order_queue]  (绑定: order.#)
                          → [topics_all_queue]    (绑定: #)
```

**队列配置**：

| 队列名称 | 绑定模式 | 匹配示例 |
|----------|---------|---------|
| `topics_user_queue` | `user.*` | user.create, user.update, user.delete |
| `topics_order_queue` | `order.#` | order.create, order.pay, order.pay.success |
| `topics_create_queue` | `*.create` | user.create, order.create, product.create |
| `topics_all_queue` | `#` | 所有路由键 |

**适用场景**：复杂的消息路由、事件驱动架构。

**API接口**：`POST /api/Topics`

## 运行说明

### 1. 环境准备

确保已安装并运行 RabbitMQ：

```bash
# Windows（使用 Chocolatey）
choco install rabbitmq

# 启动RabbitMQ服务
rabbitmq-server

# 启用管理插件（可选）
rabbitmq-plugins enable rabbitmq_management
```

**默认连接配置**：
- Host: `localhost`
- Port: `5672`
- Username: `guest`
- Password: `guest`

> **注意**：guest用户仅允许localhost连接，生产环境请创建专用用户。

### 2. 构建项目

```bash
# 进入项目目录
cd RabbitMQDemo8

# 构建所有项目
dotnet build
```

### 3. 启动生产者（WebApi）

```bash
cd RabbitMQ.Producer
dotnet run
```

启动后访问 Swagger 文档：`http://localhost:5107/swagger`

### 4. 启动消费者（控制台）

```bash
cd RabbitMQ.Consumer
dotnet run
```

根据菜单选择要启动的消费者模式。

### 5. 死信队列消费者

启动后选择死信队列消费者模式，可以查看和处理进入死信队列的消息。

**操作命令**：
- `r`：重新投递到原队列
- `d`：删除消息（确认）
- `s`：跳过（不确认，消息仍在队列中）

## API接口示例

### 简单模式

```bash
POST http://localhost:5107/api/SimpleMode
Content-Type: application/json

{
  "message": "Hello RabbitMQ!"
}
```

### 工作队列模式

```bash
POST http://localhost:5107/api/WorkQueues
Content-Type: application/json

{
  "message": "Task 123"
}
```

### 发布/订阅模式

```bash
POST http://localhost:5107/api/PubSub
Content-Type: application/json

{
  "message": "Broadcast message"
}
```

### 路由模式

```bash
POST http://localhost:5107/api/Routing
Content-Type: application/json

{
  "message": "Error occurred",
  "routingKey": "error"
}
```

### 主题模式

```bash
POST http://localhost:5107/api/Topics
Content-Type: application/json

{
  "message": "User created",
  "topic": "user.create"
}
```

## 固定队列设计说明

Fanout、Direct、Topics 模式使用固定队列名称，而非临时队列（`exclusive: true`），原因如下：

1. **防止消息丢失**：消费者未启动时，消息可存储在队列中
2. **支持后续消费**：消费者启动后可消费累积的消息
3. **持久化保证**：队列持久化，RabbitMQ重启后队列保留

**队列初始化时机**：
- 通过 `RabbitMQInitializer`（IHostedService）在应用启动时初始化
- 队列和交换器声明是幂等操作，已存在时不会重新创建

## 消息可靠性保证总结

| 环节 | 保证措施 |
|------|---------|
| 消息发送 | 发布确认（ConfirmSelect）+ 超时机制 |
| 消息存储 | 队列持久化 + 交换器持久化 + 消息持久化 |
| 消息路由 | 固定队列设计，防止路由失败 |
| 消息消费 | 手动确认（BasicAck）+ 异常处理（BasicNack） |
| 异常恢复 | 有限重试+指数退避，超过重试次数进入死信队列 |
| 死信处理 | 死信队列归档，支持重新投递、删除、跳过操作 |
| 资源管理 | 连接池复用连接和通道 |

## 生产环境建议

1. **配置管理**：使用环境变量或配置中心管理RabbitMQ连接参数，避免硬编码
2. **死信队列**：实现死信队列（DLX），对不可恢复异常的消息进行归档和分析
3. **重试策略**：实现有限重试+退避策略，避免无限重投递
4. **监控告警**：集成监控系统，监控队列长度、消息延迟、消费者状态
5. **连接池管理**：实现连接池，复用连接资源
6. **日志记录**：完善日志记录，便于问题排查
7. **高可用部署**：配置RabbitMQ集群，确保服务可用性
8. **权限管理**：创建专用用户，限制不同应用的访问权限

## License

MIT License
