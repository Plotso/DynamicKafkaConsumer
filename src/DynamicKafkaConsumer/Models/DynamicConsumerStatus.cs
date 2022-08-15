namespace DynamicKafkaConsumer.Models;

public record DynamicConsumerStatus(bool IsRunning, IEnumerable<string> CurrentlyConfiguredKeys);