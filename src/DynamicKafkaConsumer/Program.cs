using System.Text.Json;
using Confluent.Kafka;
using DynamicKafkaConsumer;
using DynamicKafkaConsumer.Consumers;
using DynamicKafkaConsumer.Consumers.BasicConsumers;
using DynamicKafkaConsumer.Contracts;
using DynamicKafkaConsumer.Producers;
using DynamicKafkaConsumer.Services;
using DynamicKafkaConsumer.Services.BasicConsumersBackgroundServices;
using KafkaCommon;
using KafkaCommon.Abstractions;
using KafkaCommon.Serialization;
using KafkaCommon.Services.Consumers.BasicConsumer;
using KafkaCommon.Services.Consumers.DynamicConsumer;
using KafkaCommon.Services.Consumers.Interfaces;
using Microsoft.Extensions.DependencyInjection.Extensions;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

var configuration = builder.Configuration;
var services = builder.Services;

services.AddControllers();
// Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
services
    .AddEndpointsApiExplorer()
    .AddSwaggerGen();

// injecting configuration to be used for dynamic consumer
//services.Configure<KafkaConfiguration>(configuration.GetSection("kafka"));
services.AddKafkaConfiguration(configuration, Constants.KafkaConfigurationSectionName);
services.AddSingleton<IMessageProcessor<string, SportInfoMessage>, SportInfoMessageProcessor>();
services.TryAddSingleton<IConsumerEventsHandler, KafkaEventsHandler>();

var serializerOptions = SerializerOptions.ConfigureSerializerOptions();
services.AddSingleton(serializerOptions);
/*
services
    .AddConsumerService<string, SportInfoMessage, MainConsumer, SportInfoMessageProcessor,
        KafkaEventsHandler, DeserializerInstances.Utf8Deserializer, DeserializerInstances.JsonDeserializer<SportInfoMessage>>(
        configuration,
        nameof(MainConsumer),
        shouldSkipMessageProcessorIfAlreadyRegistered: true); // Processor is already registered right above and we don't want duplicated logic from exactly the same processor
*/
services
    .AddDynamicConsumerService<string, SportInfoMessage, DynamicConsumer, SportInfoMessageProcessor,
        KafkaEventsHandler, DeserializerInstances.Utf8Deserializer, JsonValueSerializer<SportInfoMessage>>(
        configuration,
        shouldSkipMessageProcessorIfAlreadyRegistered: true, // Processor is already registered right above and we don't want duplicated logic from exactly the same processor
        addKafkaConfigurationWithDefaultSectionName: false);

// Register basic consumers
services
    .AddHostedService<ExampleBasicConsumerWorker>()
    .AddHostedService<ExampleBasicDynamicConsumerWorker>()
    .AddBasicKafkaConsumer<string, SportInfoMessage, ExampleBasicConsumer, KafkaEventsHandler>(SerializerOptions
        .ConfigureSerializerOptions())
    .AddBasicDynamicKafkaConsumer<string, SportInfoMessage, ExampleBasicDynamicConsumer, KafkaEventsHandler,
        DynamicConsumerModifier<string>>(SerializerOptions.ConfigureSerializerOptions());

//Register basic producerr
services.AddSingleton<BasicSportInfoMessageProducer>();

var app = builder.Build();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();