namespace KafkaCommon.ClientBuilders;

using Configuration;
using Confluent.Kafka;

public class ConsumerBuilderTopic<TKey, TValue> : ConsumerBuilder<TKey, TValue>
{
    public ConsumerBuilderTopic(TopicConfiguration configuration, string configurationSectionName) 
        : this(configuration.Settings, configuration.Topics, configurationSectionName)
    { }
    public ConsumerBuilderTopic(IEnumerable<KeyValuePair<string, string>> config, List<string> topics, string configurationSectionName) 
        : base(ReplaceConsumerGroupPlaceholders(config))
    {
        Topics = topics;
        ConfigurationSectionName = configurationSectionName;
    }
    
    public string ConfigurationSectionName { get; set; }
    
    public List<string> Topics { get; }
    
    public static IEnumerable<KeyValuePair<string, string>> ReplaceConsumerGroupPlaceholders(IEnumerable<KeyValuePair<string, string>> config) {
        const string GroupIdKey = "group.id";
        var newConfig =
            config.ToDictionary(s => s.Key, s => s.Value);

        if (newConfig.TryGetValue(GroupIdKey, out var value))
            newConfig[GroupIdKey] = FillInVariableValues(value);

        return newConfig;
    }
    
    /// <summary>
    ///  Replaces [Guid] placeholder with actual guid
    /// </summary>
    private static string FillInVariableValues(string value) 
        => value.Replace(Constants.GroupIdGuidPlaceholder, Guid.NewGuid().ToString("n").Substring(0, 13));
}