namespace KafkaCommon.Extensions;

using Confluent.Kafka;

internal static class ErrorExtensions
{
    public static string ToLogMessage(this Error e) 
        => $"Error Code: {e.Code} {Environment.NewLine}"
           + $"Error Reason: {e.Reason} {Environment.NewLine}"
           + $"IsFatal: {e.IsFatal} {Environment.NewLine}"
           + $"IsBrokerError: {e.IsBrokerError} {Environment.NewLine}"
           + $"IsLocalError: {e.IsLocalError} {Environment.NewLine}";
}