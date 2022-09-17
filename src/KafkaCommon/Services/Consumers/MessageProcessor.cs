namespace KafkaCommon.Services.Consumers;

using Confluent.Kafka;
using Interfaces;
using Microsoft.Extensions.Logging;

public abstract class MessageProcessor<TKey, TValue> : IMessageProcessor<TKey, TValue>
{
    private readonly ILogger _logger;

    public MessageProcessor(ILogger logger)
    {
        _logger = logger;
    }
    public abstract Task<bool> ReadyForProcessingMessages(CancellationToken cancellationToken = default);

    public ValueTask<bool> ShouldProcessMessage(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken = default)
    {
        if (consumeResult is null)
        {
            _logger.LogError("Skipping message consumption due to consumeResult = null");
            return new ValueTask<bool>(false);
        }

        if (consumeResult.Message is null)
        {
            _logger.LogError("Skipping message consumption due to consumeResult.Message = null");
            return new ValueTask<bool>(false);
        }

        if (consumeResult.Message.Value is null)
        {
            _logger.LogError(
                "Skipping message consumption due to consumeResult.Message.Value = null for key {MessageKey}",
                consumeResult.Message.Key);
            return new ValueTask<bool>(false);
        }
        return new ValueTask<bool>(true);
    }

    public async Task<bool> TryProcessMessage(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken = default)
    {
        var message = consumeResult.Message;
        try
        {
            await Process(consumeResult, cancellationToken);
            return true;
        }
        catch (Exception e)
        {
            _logger.LogError(
                message: string.Format("An error occured during processing of message with key: {MessageKey}", message.Key),
                exception: e);
            return false;
        }
    }
    
    protected abstract Task Process(ConsumeResult<TKey, TValue> consumeResult, CancellationToken cancellationToken = default);
}