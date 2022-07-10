namespace KafkaCommon;

using Abstractions;
using ClientBuilders;
using Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

public static class DependencyInjection
{
    public static IServiceCollection AddKafkaConfiguration(this IServiceCollection serviceCollection, IConfiguration configuration, string configKey) 
        => serviceCollection.Configure<KafkaConfiguration>(configuration.GetSection(configKey));

    public static KafkaConfiguration GetKafkaConfiguration(this IConfiguration configuration, string configKey)
    {
        var kafkaInstanceConfig = configuration.GetSection(configKey).Get<KafkaConfiguration>();
        if (kafkaInstanceConfig is null)
            throw new ArgumentException($"Unable to find section '{configKey}'");
        //Apply more specific validations if needed, for the sake this example app it's enough
        
        return kafkaInstanceConfig;
    }
    
    public static IServiceCollection AddConsumer<TKey, TMessage, TEventsHandler, TKeyDeserializer, TValueDeserializer>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName)
        where TEventsHandler : class, IConsumerEventsHandler
        where TKeyDeserializer : class, IDeserializer<TKey>
        where TValueDeserializer : class, IDeserializer<TMessage>
    {
        serviceCollection.TryAddSingleton<TEventsHandler>();
        serviceCollection.TryAddSingleton<TKeyDeserializer>();
        serviceCollection.TryAddSingleton<TValueDeserializer>();
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                    "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                    Constants.KafkaConfigurationSectionName, consumerConfiguration));

        return serviceCollection.AddSingleton(provider =>
            StaticConsumerBuilder.BuildConsumer(consumerConfiguration,
                provider.GetService<TKeyDeserializer>(),
                provider.GetService<TValueDeserializer>(),
                provider.GetService<TEventsHandler>()));
    }
    
    public static IServiceCollection AddProducer<TKey, TMessage, TEventsHandler, TKeySerializer, TValueSerializer>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string producerConfigurationName)
        where TEventsHandler : class, ISharedEventsHandler
        where TKeySerializer : class, ISerializer<TKey>
        where TValueSerializer : class, ISerializer<TMessage>
    {
        serviceCollection.TryAddSingleton<TEventsHandler>();
        serviceCollection.TryAddSingleton<TKeySerializer>();
        serviceCollection.TryAddSingleton<TValueSerializer>();
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Producers.TryGetValue(producerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));

        return serviceCollection.AddSingleton(provider =>
            StaticProducerBuilder.BuildProducer(consumerConfiguration,
                provider.GetService<TKeySerializer>(),
                provider.GetService<TValueSerializer>(),
                provider.GetService<TEventsHandler>()));
    }
}