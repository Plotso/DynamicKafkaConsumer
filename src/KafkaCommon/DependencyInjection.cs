namespace KafkaCommon;

using Abstractions;
using ClientBuilders;
using Configuration;
using Confluent.Kafka;
using Extensions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Services.Consumers;
using Services.Consumers.DynamicConsumer;
using Services.Consumers.Interfaces;

public static class DependencyInjection
{
    public static IServiceCollection AddMessageProcessor<TKey, TMessage, TMessageProcessor>(
        this IServiceCollection serviceCollection)
        where TMessageProcessor : class, IMessageProcessor<TKey, TMessage> 
        => serviceCollection.AddSingleton<IMessageProcessor<TKey, TMessage>, TMessageProcessor>();

    /// <summary>
    /// Registers dynamic consumer service with a single message processor.
    /// Consumer is dynamic in terms that it can dynamically/remotely be configured when and what messages to process.
    /// DynamicConsumer supports more than one message processors, if you want to add more, use <see cref="AddMessageProcessor{TKey,TMessage,TMessageProcessor}"/> method
    /// </summary>
    /// <param name="shouldSkipMessageProcessorIfAlreadyRegistered">
    /// Indicates whether message processor registration is skipped in case of already existing processor for given key/value type pair
    /// </param>
    /// <param name="addKafkaConfigurationWithDefaultSectionName">
    /// If set to true, method would also inject IOptionsMonitor kafka configuration from configuration section with key<see cref="Constants.KafkaConfigurationSectionName"/>
    /// If another section is desired, set param to false and use <see cref="AddKafkaConfiguration"/>
    /// </param>
    /// <typeparam name="TWorker">The class to which a hosted consumer service should be started. It's recommended to inherit <see cref="AsyncDynamicConsumer{TKey,TValue}"/></typeparam>
    /// <typeparam name="TKey">Kafka message key type</typeparam>
    /// <typeparam name="TMessage">Kafka message value type</typeparam>
    /// <typeparam name="TEventsHandler">
    /// Events handler to be applied to the consumer. Might be null. If not sure what to use, simply add <see cref="KafkaEventsHandler"/>
    /// </typeparam>
    /// <typeparam name="TMessageProcessor">
    /// Message processor that would be used for processing messages. It's recommended to inherit <see cref="MessageProcessor{TKey,TValue}"/>
    /// </typeparam>
    public static IServiceCollection AddDynamicConsumerService<TKey, TMessage, TWorker, TMessageProcessor, TEventsHandler, TKeyDeserializer, TValueDeserializer>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName,
        bool shouldSkipMessageProcessorIfAlreadyRegistered = true,
        bool addKafkaConfigurationWithDefaultSectionName = false)
        where TWorker : class, IDynamicConsumerService
        where TEventsHandler : class, IConsumerEventsHandler 
        where TMessageProcessor : class, IMessageProcessor<TKey, TMessage>
        where TKeyDeserializer : class, IDeserializer<TKey>
        where TValueDeserializer : class, IDeserializer<TMessage>
    {
        if (addKafkaConfigurationWithDefaultSectionName)
            serviceCollection.AddKafkaConfiguration(configuration, Constants.KafkaConfigurationSectionName);

        if (shouldSkipMessageProcessorIfAlreadyRegistered)
        {
            serviceCollection.TryAddSingleton<IMessageProcessor<TKey, TMessage>, TMessageProcessor>();
        }
        else
        {
            serviceCollection.AddMessageProcessor<TKey, TMessage, TMessageProcessor>();
        }
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        
        serviceCollection.TryAddSingleton<IConsumerEventsHandler, TEventsHandler>();
        serviceCollection.TryAddSingleton<TKeyDeserializer>();
        serviceCollection.TryAddSingleton<TValueDeserializer>();
        serviceCollection.TryAddSingleton<IDynamicConsumerModifier<TKey>, DynamicConsumerModifier<TKey>>();
        serviceCollection
            .AddHostedService<TWorker>();
        
        return serviceCollection;
    } 
    
        /// <summary>
    /// Registers dynamic consumer service with a single message processor.
    /// Consumer is dynamic in terms that it can dynamically/remotely be configured when and what messages to process.
    /// DynamicConsumer supports more than one message processors, if you want to add more, use <see cref="AddMessageProcessor{TKey,TMessage,TMessageProcessor}"/> method
    /// </summary>
    /// <param name="shouldSkipMessageProcessorIfAlreadyRegistered">
    /// Indicates whether message processor registration is skipped in case of already existing processor for given key/value type pair
    /// </param>
    /// <param name="addKafkaConfigurationWithDefaultSectionName">
    /// If set to true, method would also inject IOptionsMonitor kafka configuration from configuration section with key<see cref="Constants.KafkaConfigurationSectionName"/>
    /// If another section is desired, set param to false and use <see cref="AddKafkaConfiguration"/>
    /// </param>
    /// <typeparam name="TWorker">The class to which a hosted consumer service should be started. It's recommended to inherit <see cref="AsyncDynamicConsumer{TKey,TValue}"/></typeparam>
    /// <typeparam name="TKey">Kafka message key type</typeparam>
    /// <typeparam name="TMessage">Kafka message value type</typeparam>
    /// <typeparam name="TEventsHandler">
    /// Events handler to be applied to the consumer. Might be null. If not sure what to use, simply add <see cref="KafkaEventsHandler"/>
    /// </typeparam>
    /// <typeparam name="TMessageProcessor">
    /// Message processor that would be used for processing messages. It's recommended to inherit <see cref="MessageProcessor{TKey,TValue}"/>
    /// </typeparam>
    public static IServiceCollection AddDynamicConsumerServiceWithDefaultDeserializers<TKey, TMessage, TWorker, TMessageProcessor, TEventsHandler>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName,
        bool shouldSkipMessageProcessorIfAlreadyRegistered = true,
        bool addKafkaConfigurationWithDefaultSectionName = false)
        where TWorker : class, IDynamicConsumerService
        where TEventsHandler : class, IConsumerEventsHandler 
        where TMessageProcessor : class, IMessageProcessor<TKey, TMessage>
    {
        if (addKafkaConfigurationWithDefaultSectionName)
            serviceCollection.AddKafkaConfiguration(configuration, Constants.KafkaConfigurationSectionName);

        if (shouldSkipMessageProcessorIfAlreadyRegistered)
        {
            serviceCollection.TryAddSingleton<IMessageProcessor<TKey, TMessage>, TMessageProcessor>();
        }
        else
        {
            serviceCollection.AddMessageProcessor<TKey, TMessage, TMessageProcessor>();
        }
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        
        serviceCollection.TryAddSingleton<IConsumerEventsHandler, TEventsHandler>();
        serviceCollection.TryAddSingleton<IDynamicConsumerModifier<TKey>, DynamicConsumerModifier<TKey>>();
        serviceCollection.AddHostedService<TWorker>();
        
        return serviceCollection;
    } 
    
    /// <summary>
    /// Adds a kafka consumer with default deserialisers.
    /// <see cref="KafkaConfiguration"/> is required for this method to operate. If missing, add it with <see cref="AddKafkaConfiguration"/>
    /// Note: Consumer supports more than one message processors, if you want to add more, use <see cref="AddMessageProcessor{TKey,TMessage,TMessageProcessor}"/> method
    /// </summary>
    /// <param name="consumerConfigurationName">Name of specific consumer configuration section</param>
    /// <param name="shouldSkipMessageProcessorIfAlreadyRegistered">
    /// Indicates whether message processor registration is skipped in case of already existing processor for given key/value type pair
    /// </param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    /// <typeparam name="TWorker">The class to which a hosted consumer service should be started. It's recommended to inherit <see cref="AsyncConsumer{TKey,TValue}"/></typeparam>
    /// <typeparam name="TEventsHandler">
    /// Events handler to be applied to the consumer. Might be null. If not sure what to use, simply add <see cref="KafkaEventsHandler"/>
    /// </typeparam>
    /// <typeparam name="TMessageProcessor">
    /// Message processor that would be used for processing messages. It's recommended to inherit <see cref="MessageProcessor{TKey,TValue}"/>
    /// </typeparam>
    /// <exception cref="InvalidOperationException">
    /// Thrown in case <see cref="consumerConfigurationName"/> is missing from <see cref="KafkaConfiguration"/>
    /// </exception>
    public static IServiceCollection AddConsumerServiceWithDefaultDeserializers<TKey, TMessage, TWorker, TMessageProcessor, TEventsHandler>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName,
        bool shouldSkipMessageProcessorIfAlreadyRegistered = true,
        bool addKafkaConfigurationWithDefaultSectionName = false)
        where TWorker : class, IHostedService
        where TMessageProcessor : class, IMessageProcessor<TKey, TMessage>
        where TEventsHandler : class, IConsumerEventsHandler
    {
        if (addKafkaConfigurationWithDefaultSectionName)
            serviceCollection.AddKafkaConfiguration(configuration, Constants.KafkaConfigurationSectionName);
        
        serviceCollection.TryAddSingleton<TEventsHandler>();

        if (shouldSkipMessageProcessorIfAlreadyRegistered)
        {
            serviceCollection.TryAddSingleton<IMessageProcessor<TKey, TMessage>, TMessageProcessor>();
        }
        else
        {
            serviceCollection.AddMessageProcessor<TKey, TMessage, TMessageProcessor>();
        }
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));
        
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        return serviceCollection
            .AddSingleton(provider =>
                StaticConsumerBuilder.AddConsumerBuilder<TKey, TMessage>(consumerConfiguration,
                    keyDeserializer: null,
                    valueDeserializer: null,
                    provider.GetService<TEventsHandler>(),
                    consumerConfigurationName))
            .AddHostedService<TWorker>();
    }
    
    /// <summary>
    /// Adds a kafka consumer with specific deserializers.
    /// <see cref="KafkaConfiguration"/> is required for this method to operate. If missing, add it with <see cref="AddKafkaConfiguration"/>
    /// Note: Consumer supports more than one message processors, if you want to add more, use <see cref="AddMessageProcessor{TKey,TMessage,TMessageProcessor}"/> method
    /// </summary>
    /// <param name="consumerConfigurationName">Name of specific consumer configuration section</param>
    /// <param name="shouldSkipMessageProcessorIfAlreadyRegistered">
    /// Indicates whether message processor registration is skipped in case of already existing processor for given key/value type pair
    /// </param>
    /// <typeparam name="TKey"></typeparam>
    /// <typeparam name="TMessage"></typeparam>
    /// <typeparam name="TWorker">The class to which a hosted consumer service should be started. It's recommended to inherit <see cref="AsyncConsumer{TKey,TValue}"/></typeparam>
    /// <typeparam name="TEventsHandler">
    /// Events handler to be applied to the consumer. Might be null. If not sure what to use, simply add <see cref="KafkaEventsHandler"/>
    /// </typeparam>
    /// <typeparam name="TMessageProcessor">
    /// Message processor that would be used for processing messages. It's recommended to inherit <see cref="MessageProcessor{TKey,TValue}"/>
    /// </typeparam>
    /// <exception cref="InvalidOperationException">
    /// Thrown in case <see cref="consumerConfigurationName"/> is missing from <see cref="KafkaConfiguration"/>
    /// </exception>
    public static IServiceCollection AddConsumerService<TKey, TMessage, TWorker, TMessageProcessor, TEventsHandler, TKeyDeserializer, TValueDeserializer>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName,
        bool shouldSkipMessageProcessorIfAlreadyRegistered = true,
        bool addKafkaConfigurationWithDefaultSectionName = false)
        where TWorker : class, IHostedService
        where TMessageProcessor : class, IMessageProcessor<TKey, TMessage>
        where TEventsHandler : class, IConsumerEventsHandler
        where TKeyDeserializer : class, IDeserializer<TKey>
        where TValueDeserializer : class, IDeserializer<TMessage>
    {
        if (addKafkaConfigurationWithDefaultSectionName)
            serviceCollection.AddKafkaConfiguration(configuration, Constants.KafkaConfigurationSectionName);
        
        serviceCollection.TryAddSingleton<TKeyDeserializer>();
        serviceCollection.TryAddSingleton<TValueDeserializer>();
        serviceCollection.TryAddSingleton<TEventsHandler>();

        if (shouldSkipMessageProcessorIfAlreadyRegistered)
        {
            serviceCollection.TryAddSingleton<IMessageProcessor<TKey, TMessage>, TMessageProcessor>();
        }
        else
        {
            serviceCollection.AddMessageProcessor<TKey, TMessage, TMessageProcessor>();
        }
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));
        
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        return serviceCollection
            .AddSingleton(provider =>
                StaticConsumerBuilder.AddConsumerBuilder(consumerConfiguration,
                    provider.GetService<TKeyDeserializer>(),
                    provider.GetService<TValueDeserializer>(),
                    provider.GetService<TEventsHandler>(),
                    consumerConfigurationName))
            .AddHostedService<TWorker>();
    }
    
    public static void AddKafkaConfiguration(this IServiceCollection serviceCollection, IConfiguration configuration, string configKey) 
        => serviceCollection.Configure<KafkaConfiguration>(configuration.GetSection(configKey));

    public static KafkaConfiguration GetKafkaConfiguration(this IConfiguration configuration, string configKey)
    {
        var kafkaInstanceConfig = configuration.GetSection(configKey).Get<KafkaConfiguration>();
        if (kafkaInstanceConfig is null)
            throw new ArgumentException($"Unable to find section '{configKey}'");
        //Apply more specific validations if needed, for the sake this example app it's enough
        
        return kafkaInstanceConfig;
    }
    
    /* -------------------------------------------------------------------------------------------------------------------------
     *                                          !!!!!!!!!!!!  IMPORTANT !!!!!!!!!!!!
     *  Methods below directly inject IConsumer/IProducer and
     * are not planned to be used for the examples of this solution with AsyncConsumerService/AsyncDynamicConsumerService
     * that are operating via specific builders.
     *
     * They are left here to play with them if you want. Can be useful in more casual approach without the builders.
     * -------------------------------------------------------------------------------------------------------------------------
     */
    
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
        if (!kafkaConfig.Producers.TryGetValue(producerConfigurationName, out var producerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for producer {producer}",
                Constants.KafkaConfigurationSectionName, producerConfiguration));
        
        MergeKafkaConfigurations(kafkaConfig, producerConfiguration);
        return serviceCollection.AddSingleton(provider =>
            StaticProducerBuilder.BuildProducer(producerConfiguration,
                provider.GetService<TKeySerializer>(),
                provider.GetService<TValueSerializer>(),
                provider.GetService<TEventsHandler>()));
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

        if (!consumerConfiguration.Topics.Any() && kafkaConfig.BaseConfig.Topics.Any())
            consumerConfiguration.Topics = kafkaConfig.BaseConfig.Topics;
        
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        return serviceCollection.AddSingleton(provider =>
            StaticConsumerBuilder.BuildConsumer(consumerConfiguration,
                provider.GetService<TKeyDeserializer>(),
                provider.GetService<TValueDeserializer>(),
                provider.GetService<TEventsHandler>(),
                consumerConfigurationName));
    } 
    
    public static IServiceCollection AddConsumerWithDefaultDeserializers<TKey, TMessage, TEventsHandler>(
        this IServiceCollection serviceCollection,
        IConfiguration configuration, 
        string consumerConfigurationName)
        where TEventsHandler : class, IConsumerEventsHandler
    {
        serviceCollection.TryAddSingleton<TEventsHandler>();
        
        var kafkaConfig = configuration.GetKafkaConfiguration(Constants.KafkaConfigurationSectionName);
        if (!kafkaConfig.Consumers.TryGetValue(consumerConfigurationName, out var consumerConfiguration))
            throw new InvalidOperationException(string.Format(
                "There is no configuration present in kafka section {kafkaSection} for consumer {consumer}",
                Constants.KafkaConfigurationSectionName, consumerConfiguration));
        
        MergeKafkaConfigurations(kafkaConfig, consumerConfiguration);
        return serviceCollection
            .AddSingleton(provider =>
            StaticConsumerBuilder.BuildConsumer<TKey, TMessage>(consumerConfiguration,
                keyDeserializer: null,
                valueDeserializer: null,
                provider.GetService<TEventsHandler>(),
                consumerConfigurationName));
    }
    
    

    /// <summary>
    /// Merge base settings from config with consumer settings
    /// </summary>
    private static void MergeKafkaConfigurations(KafkaConfiguration config, TopicConfiguration topicConfiguration)
    {
        var baseConfig = config.BaseConfig;
        if (config == null || (baseConfig.Topics == null && baseConfig == null)) 
            return;

        if (topicConfiguration.Topics.IsNullOrEmpty() && baseConfig.Topics.IsNullOrEmpty())
            throw new ArgumentNullException("No topics provided neither in base nor topic configurations");
        
        if (topicConfiguration.Topics == null || !topicConfiguration.Topics.Any() && config.BaseConfig.Topics.Any())
            topicConfiguration.Topics = config.BaseConfig.Topics;
        
        var baseSettings = config.BaseConfig?.BaseSettings;
        if (baseSettings != null && baseSettings.Any())
        {
            foreach (var setting in baseSettings)
            {
                if (!topicConfiguration.Settings.ContainsKey(setting.Key))
                    topicConfiguration.Settings.Add(setting.Key, setting.Value);
            }
        }
    }
}