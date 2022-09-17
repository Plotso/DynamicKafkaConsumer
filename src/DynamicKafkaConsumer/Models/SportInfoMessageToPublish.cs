namespace DynamicKafkaConsumer.Models;

using Contracts;

public record SportInfoMessageToPublish(string MessageKey, SportInfoMessage Message);