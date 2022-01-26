using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitPubSubExample.Consumer;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly IConfiguration _configuration;

    public Worker(ILogger<Worker> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var connection = CreateRabbitMqConnection();
        using var channel = connection.CreateModel();

        var queue = _configuration["RabbitMQ:Queue"];

        DeclareQueue(channel, queue);

        Console.WriteLine("Appplication started, waiting for some messages...");

        var consumer = new EventingBasicConsumer(channel);
        consumer.Received += (sender, args) =>
        {
            var message = Encoding.UTF8.GetString(args.Body.ToArray());

            Console.WriteLine($"[Message Received]: '{message}'");

            channel.BasicAck(args.DeliveryTag, false);
        };

        var consumerTag = channel.BasicConsume(
            queue: queue,
            autoAck: false,
            consumer: consumer);

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(1000, stoppingToken);
        }

        channel.BasicCancel(consumerTag);

        await Task.CompletedTask;
    }

    private static void DeclareQueue(IModel channel, string queue)
    {
        channel.QueueDeclare(
            queue: queue,
            durable: false,
            exclusive: false,
            autoDelete: false,
            arguments: null
            );
    }

    private IConnection CreateRabbitMqConnection()
    {
        var connectionString = _configuration["RabbitMQ:ConnectionString"];

        var factory = new ConnectionFactory();
        factory.Uri = new Uri(connectionString);

        return factory.CreateConnection();
    }
}
