namespace DynamicKafkaConsumer.Services.BasicConsumersBackgroundServices;

using Confluent.Kafka;
using Consumers.BasicConsumers;
using Contracts;

public class ExampleBasicDynamicConsumerWorker : BackgroundService
{
    private readonly ExampleBasicDynamicConsumer _consumer;
    private readonly ILogger<ExampleBasicDynamicConsumerWorker> _logger;

    public ExampleBasicDynamicConsumerWorker(ExampleBasicDynamicConsumer consumer, ILogger<ExampleBasicDynamicConsumerWorker> logger)
    {
        _consumer = consumer;
        _logger = logger;
    }
    
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield(); // https://github.com/dotnet/runtime/issues/36063
        
        _logger.LogInformation($"{nameof(ExampleBasicDynamicConsumerWorker)} started. Once configured, it will process specific events.");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await _consumer.StartAsync(message => HandleMessage(message), stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"An error occured during message consumption by {nameof(ExampleBasicDynamicConsumerWorker)}");
            }
        }
        
        _logger.LogInformation($"Cancellation requested. {nameof(ExampleBasicConsumerWorker)} has been stopped.");
        
    }

    private Task HandleMessage(Message<string, SportInfoMessage> message)
    {
        _logger.LogInformation($"Proccesing message with key: {message.Key} & Value: {message.Value}");
        return Task.CompletedTask;
    }
}