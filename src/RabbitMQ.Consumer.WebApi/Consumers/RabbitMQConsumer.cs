using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMQ.Consumer.WebApi.Consumers;

public class RabbitMQConsumer : BackgroundService
{
    private readonly ILogger<RabbitMQConsumer> _logger;
    private readonly IConfiguration _configuration;

    public RabbitMQConsumer(ILogger<RabbitMQConsumer> logger, IConfiguration configuration)
    {
        _logger = logger;
        _configuration = configuration;
    }

    protected async override Task ExecuteAsync(CancellationToken stoppingToken)
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
    }

    // Private Methods

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