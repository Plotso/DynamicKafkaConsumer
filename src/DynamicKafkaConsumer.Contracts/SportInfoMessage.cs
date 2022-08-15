namespace DynamicKafkaConsumer.Contracts;

public class SportInfoMessage
{
    public string Provider { get; set; }
    public string RawMessage { get; set; }

    public override string ToString() => $"Provider: {Provider}, RawMessage: {RawMessage}";
}