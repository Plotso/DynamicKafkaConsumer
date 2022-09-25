namespace KafkaCommon.Extensions;

public static class ListExtensions
{
    public static bool IsNullOrEmpty(this List<string> list) => list == null || !list.Any();
}