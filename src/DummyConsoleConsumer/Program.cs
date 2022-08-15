// See https://aka.ms/new-console-template for more information
using Confluent.Kafka;
using System.Linq;
using DynamicKafkaConsumer.Contracts;
using KafkaCommon.Serialization;

Console.WriteLine("Hello, World!");

/*
if (args.Length < 3)
{
    PrintUsage();
    return;
}

var mode = args[0];
var brokerList = args[1];
var topics = args.Skip(2).ToList();
*/
var mode = "subscribe";
var brokerList = "localhost:9092,localhost:9093,localhost:9094";
var topics = new List<string> { "sport-info-messages" };

Console.WriteLine($"Started consumer, Ctrl-C to stop consuming");

CancellationTokenSource cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
    e.Cancel = true; // prevent the process from terminating.
    cts.Cancel();
};

var sameConsumerCounter = 1;

var tasks = new List<Task>();

while (!cts.IsCancellationRequested)
{
    Console.WriteLine($"Create another consumer for existing topic: >AddConsumerToGroup");
    Console.WriteLine($"Create new consumer: >NewUniqueConsumerGroup keyToProcess1 keyToProcess2 keyToProcessN");
    Console.Write("> ");

    string command;
    try
    {
        command = Console.ReadLine();

        var commandElements = command.Split(" ");
        if (commandElements[0] == "AddConsumerToGroup")
        {
            Task.Run(() => Run_Consume(brokerList, topics, cts.Token, sameConsumerCounter++));
        }
        else if (commandElements[0] == "NewUniqueConsumerGroup")
        {
            var keysToProcess = commandElements.Skip(1).Where(e => !string.IsNullOrEmpty(e));
            Task.Run(() => Custom_Run_UniqueConsume(brokerList, topics, cts.Token, keysToProcess));
        }
        else
        {
            Console.WriteLine($"Invalid command");
        }
    }
    catch (IOException)
    {
        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
        break;
    }
    if (command == null)
    {
        // Console returned null before
        // the CancelKeyPress was treated
        break;
    }
}
switch (mode)
{
    case "subscribe":
        Run_Consume(brokerList, topics, cts.Token, sameConsumerCounter++);
        break;

    case "manual":
        Run_ManualAssign(brokerList, topics, cts.Token);
        break;

    default:
        PrintUsage();
        break;
}

while (!cts.IsCancellationRequested)
{
    Console.WriteLine($"Create another consumer for existing topic: >AddConsumerToGroup");
    Console.WriteLine($"Create new consumer: >NewUniqueConsumerGroup keyToProcess1 keyToProcess2 keyToProcessN");
    Console.Write("> ");

    string command;
    try
    {
        command = Console.ReadLine();

        var commandElements = command.Split(" ");
        if (commandElements[0] == "AddConsumerToGroup")
        {
            Task.Run(() => Run_Consume(brokerList, topics, cts.Token, sameConsumerCounter++));
        }
        else if (commandElements[0] == "NewUniqueConsumerGroup")
        {
            var keysToProcess = commandElements.Skip(1).Where(e => !string.IsNullOrEmpty(e));
            Task.Run(() => Custom_Run_UniqueConsume(brokerList, topics, cts.Token, keysToProcess));
        }
        else
        {
            Console.WriteLine($"Invalid command");
        }
    }
    catch (IOException)
    {
        // IO exception is thrown when ConsoleCancelEventArgs.Cancel == true.
        break;
    }
    if (command == null)
    {
        // Console returned null before
        // the CancelKeyPress was treated
        break;
    }
}

/// <summary>
///     In this example
///         - offsets are manually committed.
///         - no extra thread is created for the Poll (Consume) loop.
/// </summary>
static void Run_Consume(string brokerList, List<string> topics, CancellationToken cancellationToken, int sameConsumerCounter)
{
    var consumerGroup = "csharp-consumer";
    var config = new ConsumerConfig
    {
        BootstrapServers = brokerList,
        GroupId = consumerGroup,
        EnableAutoCommit = false,
        StatisticsIntervalMs = 5000,
        SessionTimeoutMs = 6000,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnablePartitionEof = true,
        // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
        // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
    };

    const int commitPeriod = 5;

    // Note: If a key or value deserializer is not set (as is the case below), the
    // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
    // will be used automatically (where available). The default deserializer for string
    // is UTF8. The default deserializer for Ignore returns null for all input data
    // (including non-null data).
    using (var consumer = new ConsumerBuilder<Ignore, SportInfoMessage>(config)
        // Note: All handlers are called on the main .Consume thread.
        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        // COMMENTING OUT DUE TO SPAM
        //.SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
        .SetPartitionsAssignedHandler((c, partitions) =>
        {
            // Since a cooperative assignor (CooperativeSticky) has been configured, the
            // partition assignment is incremental (adds partitions to any existing assignment).
            Console.WriteLine(
                "Partitions incrementally assigned: [" +
                string.Join(',', partitions.Select(p => p.Partition.Value)) +
                "], all: [" +
                string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                "]");

            // Possibly manually specify start offsets by returning a list of topic/partition/offsets
            // to assign to, e.g.:
            // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
        })
        .SetPartitionsRevokedHandler((c, partitions) =>
        {
            // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
            // assignment is incremental (may remove only some partitions of the current assignment).
            var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
            Console.WriteLine(
                "Partitions incrementally revoked: [" +
                string.Join(',', partitions.Select(p => p.Partition.Value)) +
                "], remaining: [" +
                string.Join(',', remaining.Select(p => p.Partition.Value)) +
                "]");
        })
        .SetPartitionsLostHandler((c, partitions) =>
        {
            // The lost partitions handler is called when the consumer detects that it has lost ownership
            // of its assignment (fallen out of the group).
            Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
        })
        .SetValueDeserializer(new DeserializerInstances.JsonDeserializer<SportInfoMessage>())
        .Build())
    {
        consumer.Subscribe(topics);

        try
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine(
                            $"[Consumer{sameConsumerCounter}][{consumerGroup}] Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                        continue;
                    }

                    Console.WriteLine($"[Consumer{sameConsumerCounter}][{consumerGroup}] Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                    if (consumeResult.Offset % commitPeriod == 0)
                    {
                        // The Commit method sends a "commit offsets" request to the Kafka
                        // cluster and synchronously waits for the response. This is very
                        // slow compared to the rate at which the consumer is capable of
                        // consuming messages. A high performance application will typically
                        // commit offsets relatively infrequently and be designed handle
                        // duplicate messages in the event of failure.
                        try
                        {
                            consumer.Commit(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Commit error: {e.Error.Reason}");
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Closing consumer.");
            consumer.Close();
        }
    }
}

/// <summary>
///     In this example
///         - consumer group functionality (i.e. .Subscribe + offset commits) is not used.
///         - the consumer is manually assigned to a partition and always starts consumption
///           from a specific offset (0).
/// </summary>
static void Run_ManualAssign(string brokerList, List<string> topics, CancellationToken cancellationToken)
{
    var config = new ConsumerConfig
    {
        // the group.id property must be specified when creating a consumer, even
        // if you do not intend to use any consumer group functionality.
        GroupId = new Guid().ToString(),
        BootstrapServers = brokerList,
        // partition offsets can be committed to a group even by consumers not
        // subscribed to the group. in this example, auto commit is disabled
        // to prevent this from occurring.
        EnableAutoCommit = true
    };

    using (var consumer =
        new ConsumerBuilder<Ignore, string>(config)
            .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
            .Build())
    {
        consumer.Assign(topics.Select(topic => new TopicPartitionOffset(topic, 0, Offset.Beginning)).ToList());

        try
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(cancellationToken);
                    // Note: End of partition notification has not been enabled, so
                    // it is guaranteed that the ConsumeResult instance corresponds
                    // to a Message, and not a PartitionEOF event.
                    Console.WriteLine($"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Closing consumer.");
            consumer.Close();
        }
    }
}

static void PrintUsage()
    => Console.WriteLine("Usage: .. <subscribe|manual> <broker,broker,..> <topic> [topic..]");

/// <summary>
///     In this example
///         - offsets are manually committed.
///         - no extra thread is created for the Poll (Consume) loop.
/// </summary>
static void Custom_Run_UniqueConsume(string brokerList, List<string> topics, CancellationToken cancellationToken, IEnumerable<string> keysToProcess)
{
    var consumerGroup = Guid.NewGuid().ToString();
    var config = new ConsumerConfig
    {
        BootstrapServers = brokerList,
        GroupId = consumerGroup,
        EnableAutoCommit = false,
        StatisticsIntervalMs = 5000,
        SessionTimeoutMs = 6000,
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnablePartitionEof = true,
        // A good introduction to the CooperativeSticky assignor and incremental rebalancing:
        // https://www.confluent.io/blog/cooperative-rebalancing-in-kafka-streams-consumer-ksqldb/
        PartitionAssignmentStrategy = PartitionAssignmentStrategy.CooperativeSticky
    };

    const int commitPeriod = 5;

    // Note: If a key or value deserializer is not set (as is the case below), the
    // deserializer corresponding to the appropriate type from Confluent.Kafka.Deserializers
    // will be used automatically (where available). The default deserializer for string
    // is UTF8. The default deserializer for Ignore returns null for all input data
    // (including non-null data).
    using (var consumer = new ConsumerBuilder<string, SportInfoMessage>(config)
        // Note: All handlers are called on the main .Consume thread.
        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
        // COMMENTING OUT DUE TO SPAM
        //.SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
        .SetPartitionsAssignedHandler((c, partitions) =>
        {
            // Since a cooperative assignor (CooperativeSticky) has been configured, the
            // partition assignment is incremental (adds partitions to any existing assignment).
            Console.WriteLine(
                "Partitions incrementally assigned: [" +
                string.Join(',', partitions.Select(p => p.Partition.Value)) +
                "], all: [" +
                string.Join(',', c.Assignment.Concat(partitions).Select(p => p.Partition.Value)) +
                "]");

            // Possibly manually specify start offsets by returning a list of topic/partition/offsets
            // to assign to, e.g.:
            // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
        })
        .SetPartitionsRevokedHandler((c, partitions) =>
        {
            // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
            // assignment is incremental (may remove only some partitions of the current assignment).
            var remaining = c.Assignment.Where(atp => partitions.Where(rtp => rtp.TopicPartition == atp).Count() == 0);
            Console.WriteLine(
                "Partitions incrementally revoked: [" +
                string.Join(',', partitions.Select(p => p.Partition.Value)) +
                "], remaining: [" +
                string.Join(',', remaining.Select(p => p.Partition.Value)) +
                "]");
        })
        .SetPartitionsLostHandler((c, partitions) =>
        {
            // The lost partitions handler is called when the consumer detects that it has lost ownership
            // of its assignment (fallen out of the group).
            Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
        })
        .SetValueDeserializer(new DeserializerInstances.JsonDeserializer<SportInfoMessage>())
        .Build())
    {
        consumer.Subscribe(topics);

        var partitionsReachedEOF = new HashSet<int>();

        try
        {
            while (true)
            {
                try
                {
                    var partitions = consumer.Assignment.Select(atp => atp.Partition.Value).ToList();
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult.IsPartitionEOF)
                    {
                        partitionsReachedEOF.Add(consumeResult.Partition.Value);
                        Console.WriteLine(
                            $"[UniqueConsumer-{consumerGroup}]Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                        if (partitions.All(partitionsReachedEOF.Contains) && partitions.Count == partitionsReachedEOF.Count)
                        {
                            var topicPartitions = consumer.Assignment;
                            Console.WriteLine($"[UniqueConsumer-{consumerGroup}] Dispose started");
                            consumer.Unsubscribe();
                            consumer.IncrementalUnassign(topicPartitions);
                            consumer.Dispose();
                            break;
                        }
                        continue;
                    }

                    if (!keysToProcess.ToList().Contains(consumeResult.Key))
                    {
                        Console.WriteLine($"[UniqueConsumer-{consumerGroup}] Skipping messages since it's not in processOnly");
                        continue;
                    }

                    Console.WriteLine($"[UniqueConsumer-{consumerGroup}]Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");

                    if (consumeResult.Offset % commitPeriod == 0)
                    {
                        // The Commit method sends a "commit offsets" request to the Kafka
                        // cluster and synchronously waits for the response. This is very
                        // slow compared to the rate at which the consumer is capable of
                        // consuming messages. A high performance application will typically
                        // commit offsets relatively infrequently and be designed handle
                        // duplicate messages in the event of failure.
                        try
                        {
                            consumer.Commit(consumeResult);
                        }
                        catch (KafkaException e)
                        {
                            Console.WriteLine($"Commit error: {e.Error.Reason}");
                        }
                    }
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Closing consumer.");
            consumer.Close();
        }
    }
}