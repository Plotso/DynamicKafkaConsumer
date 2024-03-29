﻿namespace KafkaCommon.Services.Consumers.DynamicConsumer;

using System.Collections.Concurrent;
using Interfaces;
using Microsoft.Extensions.Logging;

public class DynamicConsumerModifier<TConsumerKey> : IDynamicConsumerModifier<TConsumerKey>
{
    private ConcurrentBag<TConsumerKey> _configuredKeys;
    private readonly ILogger<DynamicConsumerModifier<TConsumerKey>> _logger;

    public DynamicConsumerModifier(ILogger<DynamicConsumerModifier<TConsumerKey>> logger)
    {
        _logger = logger;
        _configuredKeys = new ConcurrentBag<TConsumerKey>();
    }

    public bool IsDynamicConsumerActive() => _configuredKeys.Any();

    public void SetKeysToConsumer(IEnumerable<TConsumerKey> consumerKeys)
        => SafeExecute(() =>
            {
                //ToDo: Move log below to caller
                _logger.LogInformation($"Setting keys to: {string.Join(" ,", consumerKeys)}");
                _configuredKeys = new ConcurrentBag<TConsumerKey>(consumerKeys);
            },
            nameof(SetKeysToConsumer));

    public void MarkKeysAsProcessed() => _configuredKeys = new ConcurrentBag<TConsumerKey>();

    public IEnumerable<TConsumerKey> GetKeysToProcess() => _configuredKeys;

    private void SafeExecute(Action action, string actionName)
    {
        try
        {
            action();
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, $"An error occured during {actionName}");
        }
    }
}