# DynamicKafkaConsumer
Example implementation of an application that has both regular consumer and a dynamic consumer(s) that appears on demand (after http call)

The idea of the project is to show how a dynamic consumer can be created and 
easily registered in any ASP.NET app.

Contains library like registration of normal & dynamic consumers.
# Solution Projects
* **DynamicKafkaConsumer.Contracts** - contract models to be used for the purpose of demo showcase of solution
* **KafkaCommon** - library containing wrapping all kafka specific code and nugets. It contains logic for creation and registration of both normal and dynamic consumers. Also contains logic for producer. It provides DI methods that can be directly applied to any existing .NET app using service collection.
* **DynamicKafkaConsumer** - ASP.NET API showing how easily achievable is to register a normal and a dynamic consumer using KafkaCommon library logic. Consist of 1 dynamic consumer, 1 normal consumer, 1 controller that triggers/configured the dynamic consumer and 1 message processor containing logic of what to do with each kafka message when received.
* **DummyConsoleProducer** - basic console implementation of KafkaProducer but with custom logic adjusted to be useful for current solution presentation. 
Original simplified kafka producer(s) can be found in [ConfluentKafka single producer example](https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Producer/Program.cs) 
or [ConfluentKafka multi producer example](https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/MultiProducer/Program.cs)
* **DummyConsoleConsumer** - if you're not interested in the whole complexity of the WebAPI configurable app and the KafkaCommon library, 
this dummy console app is **an extended version of [ConfluentKafka example consumer](https://github.com/confluentinc/confluent-kafka-dotnet/blob/master/examples/Consumer/Program.cs) containing also logic for dynamically configurable consumer**

# [Normal Consumer example](src/DynamicKafkaConsumer/Consumers/MainConsumer.cs)
Contains example of a normal kafka consumer. Example is with a consumer using separate MessageProcessor class.
As it can be seen, KafkaCommon is designed in such way that for default consumers you won't need anything more than just creating a consumer with specific name and inheriting [AsyncConsumer](src/KafkaCommon/Services/AsyncConsumer.cs). The AsyncConsumer contains all required logic for starting a normal consumer.

# [Dynamic Consumer example](src/DynamicKafkaConsumer/Consumers/DynamicConsumer.cs)
**DynamicConsumer** name has been chosen because the consumer:
* **only processes messages when triggered**.
* **only process messages containing pre-set list of keys.**
* **is triggered remotely**, for example via API request to controller.
* **is disposed when finishes processing**
* for each request that comes, **the consumer starts with unique group.id** (Guid)
so that **it won't interfere with other currently active consumers**

It's very useful when you want to reprocess only specific messages.

When configured to process given keys, it would **work until it reaches PartitionEOF** 
for all partitions containing messages for given keys. 
**After that it will be disposed**.

In current example, it's configured via API calls to [DynamicConsumerController](src/DynamicKafkaConsumer/Controllers/DynamicConsumerController.cs).

### Initialising Dynamic Consumer
Example is with a consumer using separate MessageProcessor class.
As it can be seen, KafkaCommon is designed in such way that for default consumers you won't need anything more than just creating a consumer with specific name and inheriting [AsyncDynamicConsumer](src/KafkaCommon/Services/DynamicConsumer/AsyncDynamicConsumer.cs). The AsyncDynamicConsumer contains all required logic for starting a normal consumer.
**Only ConsumerConfigurationName must be overriden** to provide the configuration section name from which the services would read required kafka specific settings.

# [Basic Consumers](src/KafkaCommon/Services/Consumers/BasicConsumer)
Since the shown examples for Consumer/DynamicConsumer are more complex and oblige the user to use MessageProcessor, solution also offer ***a basic, more simplified, approach***.
The basic consumer use different approach to construction, leaving the build logic in the hands of the user.
The **difference** here is that for each consumer **the user needs a new Consumer inheritor in the project**.

Usage is relatively simple, implementation in service can override the ShouldProcess method and also it must pass logic of how to handle each message. Everything else is done behind the scenes in the abstract classes.
The basic implementation also provides option to consume with timeout provided rather than cancellationToken.

### NOTE: 
Basic consumers use JsonSerializer for the value with the idea of the message to be gziped on producer side.

### Example usage for basic consumers:
* Consumer classes can optionally be created with custom logic for config section & ShouldProcessMessage. [Here are examples for normal and dynamic consumer](src/DynamicKafkaConsumer/Consumers/BasicConsumers).
* Background services **must** be started as well to enable endless consumption.  [Here are examples for normal and dynamic consumers](src/DynamicKafkaConsumer/Services/BasicConsumersBackgroundServices).

# [Basic Producer](src/KafkaCommon/Services/Producers/BasicProducer.cs)
A very simple implementation for a kafka producer. It's configured what key & value to use. User must pass also value serializer.
Example usage can be found in [BasicSportInfoMessageProducer](src/DynamicKafkaConsumer/Producers/BasicSportInfoMessageProducer.cs).

The producer is used inside [](src/DynamicKafkaConsumer/Controllers/SportInfoMessageController.cs) to produce messages send to the endpoint. It's done like this for demo purposes.

# Prerequisites

In order to be sure you can run the project, make sure you have the following frameworks installed on your PC:
* **NET 6** - should be installed with VisualStudio/Rider 2022 edition(s)
* **Docker** - if you want to test with kafka provided in the solution, you need to have installed Docker. Otherwise, you need to connect to an existing kafka instance somewhere.

It's good to have the following software products installed in order to be sure the project is running as expected:
* **VisualStudio 2022 / Rider 2022** - built and tested on both of those IDEs, the project should also be running on any newer version as long as it supports the above mentioned frameworks [^1]

# Kafka Installation
The docker-compose.yml file contains required logic to start locally a 
kafka with 3 zookepers and 3 brokers.
It's a little bit modified version to the ymls that could be found in 
[Conductors' kafka-stack-docker-compose](https://github.com/conduktor/kafka-stack-docker-compose) repository.

Open cmd/powershell in folder containing the yamls file and execute following command

```shell
docker compose up
```
You will see by the logs (or by checking docker dashboard) when all services are up and running.

**NOTE**: The yml creates 3 kafka instances and also initializes a kafka topic.

Following configuration might be used to access kafka for a consumer if no modification are applied to the docker-compose file:

```json
  "kafka": {
    "config": {
      "baseSettings": {
        "bootstrap.servers": ""
      }
    },
    "consumers": {
      "MainConsumer": {
        "topicName": "sport-info-messages",
        "config": {
          "bootstrap.servers": "localhost:9092,localhost:9093,localhost:9094",
          "group.id": "DynamicKafkaConsumer.MainConsumer",
          "auto.offset.reset": "earliest"
        }
      }
    },
    "producers": {
    }
  }
```

# How to run the project
Project can easily be started on any machine that meets the requirements mentioned in Prerequisites section.

Steps to start the project:
1. Spin up kafka following Kafka installation section above or adjust [appsettings.json](src/DynamicKafkaConsumer/appsettings.json) with bootstrap server of an existing Kafka 
2. Run the project

## Built With

* [ASP.NET 6](https://docs.microsoft.com/en-us/aspnet/core/release-notes/aspnetcore-6.0?view=aspnetcore-6.0) - The web framework
* [ConfluentKafka](https://github.com/confluentinc/confluent-kafka-dotnet) - Confluent's Apache Kafka .NET client


## Contribution

* **Ilian Ganchosov** - *One man army / project creator* - [Plotso](https://github.com/Plotso)


## License

This project is licensed under the GPL-3.0 License - see the [LICENSE.md](LICENSE.md) file for details


[^1]: It's not mandatory to use the mentioned software applications. Any IDE supporting .NET 6 should do the job. Same goes for the .NET command-line interface (CLI).