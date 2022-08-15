namespace KafkaCommon.Services.Interfaces;

using DynamicConsumer;

/// <summary>
/// Used to operate remotely the dynamic consumer
/// </summary>
public interface IDynamicConsumerModifier<TConsumerKey>
{
    /// <summary>
    /// Indicated whether dynamic consumer is currently active. Intended to be used by external services that change/trigger the dynamic consumer
    /// </summary>
    bool IsDynamicConsumerActive();
    
    /// <summary>
    /// Configures collection of keys that <see cref="AsyncDynamicConsumer{TKey,TValue}"/> would process
    /// </summary>
    /// <param name="consumerKeys"></param>
    void SetKeysToConsumer(IEnumerable<TConsumerKey> consumerKeys);
    
    /// <summary>
    /// Marks currently configured keys as processed. This would disable active dynamic consumer until new configuration/trigger comes
    /// </summary>
    void MarkKeysAsProcessed();
    
    /// <summary>
    /// Method allowing <see cref="AsyncDynamicConsumer{TKey,TValue}"/> to get keys, which should be processed
    /// </summary>
    IEnumerable<TConsumerKey> GetKeysToProcess();
}