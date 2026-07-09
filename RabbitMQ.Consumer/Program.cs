using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using RabbitMQ.Consumer.Consumers;
using RabbitMQ.Shared.Constants;
using RabbitMQ.Shared.Options;
using RabbitMQ.Shared.Services;

// 构建配置
var configuration = new ConfigurationBuilder()
    .SetBasePath(AppContext.BaseDirectory)
    .AddJsonFile("appsettings.json", optional: false, reloadOnChange: true)
    .Build();

// 构建依赖注入容器
var services = new ServiceCollection();

// 注册配置
services.Configure<RabbitMQOptions>(configuration.GetSection(RabbitMQOptions.SectionName));

// 注册日志
services.AddLogging(builder =>
{
    builder.AddConsole();
    builder.SetMinimumLevel(LogLevel.Information);
});

// 注册RabbitMQ服务
services.AddSingleton<RabbitMQConnectionFactory>();
services.AddSingleton<RabbitMQConnectionPool>();

// 构建服务提供者
var serviceProvider = services.BuildServiceProvider();

Console.WriteLine("=====================================");
Console.WriteLine("    RabbitMQ 消费者客户端");
Console.WriteLine("    .NET 8 + RabbitMQ.Client");
Console.WriteLine("=====================================");
Console.WriteLine();

while (true)
{
    Console.WriteLine("请选择要启动的消费者模式:");
    Console.WriteLine("1. 简单模式 (Simple Mode)");
    Console.WriteLine("2. 工作队列模式 (Work Queues)");
    Console.WriteLine("3. 发布/订阅模式 (Pub/Sub - Fanout)");
    Console.WriteLine("4. 路由模式 (Routing - Direct)");
    Console.WriteLine("5. 主题模式 (Topics)");
    Console.WriteLine("6. 死信队列消费者 (Dead Letter Queue)");
    Console.WriteLine("0. 退出");
    Console.Write("\n请输入选项: ");

    var input = Console.ReadLine();
    switch (input)
    {
        case "1":
            RunSimpleMode(serviceProvider);
            break;
        case "2":
            RunWorkQueuesMode(serviceProvider);
            break;
        case "3":
            RunPubSubMode(serviceProvider);
            break;
        case "4":
            RunRoutingMode(serviceProvider);
            break;
        case "5":
            RunTopicsMode(serviceProvider);
            break;
        case "6":
            RunDeadLetterMode(serviceProvider);
            break;
        case "0":
            Console.WriteLine("退出程序...");
            return;
        default:
            Console.WriteLine("无效选项，请重新输入");
            break;
    }

    Console.WriteLine("\n按任意键继续...");
    Console.ReadKey();
    Console.Clear();
}

void RunSimpleMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "SimpleConsumer";
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    SimpleModeConsumer.StartConsumer(name, connectionPool);
}

void RunWorkQueuesMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "Worker";
    Console.Write("是否启用公平分发 (y/n): ");
    var fairDispatch = Console.ReadLine()?.Equals("y", StringComparison.OrdinalIgnoreCase) ?? true;
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    WorkQueuesConsumer.StartConsumer(name, fairDispatch, connectionPool);
}

void RunPubSubMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "Subscriber";
    
    Console.WriteLine("\n请选择要监听的队列:");
    for (int i = 0; i < RabbitMQConstants.FanoutQueueNames.Length; i++)
    {
        Console.WriteLine($"{i + 1}. {RabbitMQConstants.FanoutQueueNames[i]}");
    }
    Console.Write("输入选项: ");
    var queueIndex = int.TryParse(Console.ReadLine(), out var idx) && idx > 0 && idx <= RabbitMQConstants.FanoutQueueNames.Length
        ? idx - 1
        : 0;
    
    var queueName = RabbitMQConstants.FanoutQueueNames[queueIndex];
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    PubSubConsumer.StartConsumer(name, queueName, connectionPool);
}

void RunRoutingMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "RoutingConsumer";
    
    Console.WriteLine("\n请选择要监听的队列:");
    for (int i = 0; i < RabbitMQConstants.DirectQueueNames.Length; i++)
    {
        Console.WriteLine($"{i + 1}. {RabbitMQConstants.DirectQueueNames[i]}");
    }
    Console.Write("输入选项: ");
    var queueIndex = int.TryParse(Console.ReadLine(), out var idx) && idx > 0 && idx <= RabbitMQConstants.DirectQueueNames.Length
        ? idx - 1
        : 0;
    
    var queueName = RabbitMQConstants.DirectQueueNames[queueIndex];
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    RoutingConsumer.StartConsumer(name, queueName, connectionPool);
}

void RunTopicsMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "TopicsConsumer";
    
    Console.WriteLine("\n请选择要监听的队列:");
    for (int i = 0; i < RabbitMQConstants.TopicQueueNames.Length; i++)
    {
        Console.WriteLine($"{i + 1}. {RabbitMQConstants.TopicQueueNames[i]}");
    }
    Console.Write("输入选项: ");
    var queueIndex = int.TryParse(Console.ReadLine(), out var idx) && idx > 0 && idx <= RabbitMQConstants.TopicQueueNames.Length
        ? idx - 1
        : 0;
    
    var queueName = RabbitMQConstants.TopicQueueNames[queueIndex];
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    TopicsConsumer.StartConsumer(name, queueName, connectionPool);
}

void RunDeadLetterMode(IServiceProvider provider)
{
    Console.Write("\n请输入消费者名称: ");
    var name = Console.ReadLine() ?? "DeadLetterConsumer";
    var connectionPool = provider.GetRequiredService<RabbitMQConnectionPool>();
    DeadLetterConsumer.StartConsumer(name, connectionPool);
}