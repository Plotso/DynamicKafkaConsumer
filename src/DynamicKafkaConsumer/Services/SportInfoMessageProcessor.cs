namespace DynamicKafkaConsumer.Services;

using Confluent.Kafka;
using Contracts;
using KafkaCommon.Services;

public class SportInfoMessageProcessor : MessageProcessor<string, SportInfoMessage>
{
    private readonly ILogger<SportInfoMessageProcessor> _logger;

    public SportInfoMessageProcessor(ILogger<SportInfoMessageProcessor> logger) : base(logger)
    {
        _logger = logger;
    }

    public override Task<bool> ReadyForProcessingMessages(CancellationToken cancellationToken = default) 
        => Task.FromResult(true);

    protected override async Task Process(ConsumeResult<string, SportInfoMessage> consumeResult, CancellationToken cancellationToken = default)
    {
        //ToDo: Parse the raw part of the message to an actual msg
        _logger.LogInformation($"Proccesing message with key: {consumeResult.Message.Key} & Value: {consumeResult.Message.Value}");
    }
}