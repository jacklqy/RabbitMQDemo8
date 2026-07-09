# RabbitMQ Demo (.NET 8)

一个基于 .NET 8 的 RabbitMQ 消息队列完整演示项目，包含生产者（WebApi）和消费者（控制台客户端），支持五种核心模式。

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

## 技术栈

| 组件 | 版本 | 说明 |
|------|------|------|
| .NET | 8.0 | 运行时框架 |
| RabbitMQ.Client | 6.8.1 | RabbitMQ客户端库 |
| ASP.NET Core | 8.0 | WebApi框架 |
| Swashbuckle.AspNetCore | 6.5.0 | Swagger文档生成 |

## 项目结构

```
RabbitMQDemo8/
├── RabbitMQDemo.sln                    # 解决方案文件
├── RabbitMQ.Shared/                    # 共享类库（核心服务）
│   ├── Services/
│   │   ├── RabbitMQConnectionFactory.cs   # 连接工厂
│   │   └── RabbitMQService.cs             # RabbitMQ核心服务（含发布确认）
│   ├── Constants/
│   │   └── RabbitMQConstants.cs           # 队列/交换器常量配置
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
│   ├── Program.cs                         # 启动配置
│   └── appsettings.json                   # 应用配置
└── RabbitMQ.Consumer/                  # 消费者（控制台客户端）
    ├── Consumers/
    │   ├── SimpleModeConsumer.cs          # 简单模式消费者
    │   ├── WorkQueuesConsumer.cs          # 工作队列消费者
    │   ├── PubSubConsumer.cs              # 发布/订阅消费者
    │   ├── RoutingConsumer.cs             # 路由模式消费者
    │   └── TopicsConsumer.cs              # 主题模式消费者
    └── Program.cs                         # 消费者入口
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

### 3. 异常分类策略

| 异常类型 | 是否重新入队 | 说明 |
|----------|-------------|------|
| `InvalidOperationException` | ❌ 否 | 不可恢复异常（如参数错误），丢弃消息 |
| 其他异常 | ✅ 是 | 可重试异常（如网络抖动），重新入队 |

> **生产环境建议**：实现更精细的异常分类、添加重试次数限制、对不可恢复异常发送到死信队列

### 4. 消息持久化

确保RabbitMQ重启后消息不丢失：

| 持久化级别 | 设置方式 | 说明 |
|-----------|---------|------|
| 队列持久化 | `durable: true` | 队列在RabbitMQ重启后保留 |
| 交换器持久化 | `durable: true` | 交换器在RabbitMQ重启后保留 |
| 消息持久化 | `Persistent = true` | 消息写入磁盘 |

### 5. 公平分发（BasicQos）

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
| 异常恢复 | 可重试异常重新入队，不可恢复异常丢弃 |

## 生产环境建议

1. **配置管理**：将RabbitMQ连接配置（Host、Port、Username、Password）从代码中移出，使用配置文件或环境变量
2. **死信队列**：实现死信队列（DLX），对不可恢复异常的消息进行归档和分析
3. **重试策略**：实现有限重试+退避策略，避免无限重投递
4. **监控告警**：集成监控系统，监控队列长度、消息延迟、消费者状态
5. **连接池管理**：实现连接池，复用连接资源
6. **日志记录**：完善日志记录，便于问题排查

## License

MIT License
