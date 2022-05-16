using System.Text;
using RabbitMQ.Client;

namespace RabbitMQ.Producer.ConsoleApp;

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

        Console.WriteLine("Appplication started, type some messages:");

        while (!stoppingToken.IsCancellationRequested)
        {
            var inputMessage = Console.ReadLine();

            if (!string.IsNullOrWhiteSpace(inputMessage))
            {
                PublishMessage(channel, queue, inputMessage);

                _logger.LogInformation($"[Message Published]: '{inputMessage}'");
            }
        }

        await Task.CompletedTask;
    }

    private static void PublishMessage(IModel channel, string queue, string inputMessage)
    {
        var inputStream = Encoding.UTF8.GetBytes(inputMessage);

        channel.BasicPublish(
            exchange: "",
            routingKey: queue,
            basicProperties: null,
            body: inputStream
            );
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
