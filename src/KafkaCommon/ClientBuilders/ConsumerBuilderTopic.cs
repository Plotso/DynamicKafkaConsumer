namespace KafkaCommon.ClientBuilders;

using Configuration;
using Confluent.Kafka;

public class ConsumerBuilderTopic<TKey, TValue> : ConsumerBuilder<TKey, TValue>
{
    public ConsumerBuilderTopic(TopicConfiguration configuration) 
        : this(configuration.Settings, configuration.Topics)
    { }
    public ConsumerBuilderTopic(IEnumerable<KeyValuePair<string, string>> config, List<string> topics) 
        : base(ReplaceConsumerGroupPlaceholders(config))
    {
        Topics = topics;
    }
    
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