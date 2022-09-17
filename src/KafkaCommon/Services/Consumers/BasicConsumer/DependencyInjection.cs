namespace KafkaCommon.Services.Consumers.BasicConsumer;

using System.Text.Json;
using Interfaces;
using KafkaCommon.Abstractions;
using KafkaCommon.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

/// <summary>
/// Separate dependency injection so that methods won't be confused with the ones in the other DI. It's cleaner this way.
/// NOTE: KafkaConfiguration injections are handled from other DependencyInjection.cs
/// </summary>
public static class DependencyInjection
{
    public static IServiceCollection AddBasicKafkaConsumer<TKey, TValue, TEnhancedConsumer, TEventsHandler>(
        this IServiceCollection serviceCollection, JsonSerializerOptions jsonSerializerOptions = null)
        where TValue : class
        where TEnhancedConsumer : BasicConsumer<TKey, TValue>
        where TEventsHandler : class, IConsumerEventsHandler 
    {
        serviceCollection.TryAddSingleton<IConsumerEventsHandler, TEventsHandler>();
        serviceCollection.TryAddSingleton<TEventsHandler>();
        serviceCollection.TryAddSingleton((sp) => new JsonValueSerializer<TValue>(jsonSerializerOptions));
        return serviceCollection.AddSingleton<TEnhancedConsumer>();
    }

    /// <summary>
    /// Register custom dynamic consumer inheritor of EnhancedConsumer
    /// </summary>
    public static IServiceCollection AddBasicDynamicKafkaConsumer<TKey, TValue, TDynamicConsumer, TEventsHandler, TDynamicConsumerModifier>(
        this IServiceCollection serviceCollection, JsonSerializerOptions jsonSerializerOptions = null)
        where TValue : class
        where TDynamicConsumer : BasicDynamicConsumer<TKey, TValue>
        where TEventsHandler : class, IConsumerEventsHandler 
        where TDynamicConsumerModifier : class, IDynamicConsumerModifier<TKey>
    {
        serviceCollection.TryAddSingleton<IConsumerEventsHandler, TEventsHandler>();
        serviceCollection.TryAddSingleton<IDynamicConsumerModifier<TKey>, TDynamicConsumerModifier>();
        serviceCollection.TryAddSingleton((sp) => new JsonValueSerializer<TValue>(jsonSerializerOptions));
        return serviceCollection.AddSingleton<TDynamicConsumer>();
    }
}