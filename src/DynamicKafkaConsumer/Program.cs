using Confluent.Kafka;
using DynamicKafkaConsumer.Consumers;
using DynamicKafkaConsumer.Contracts;
using DynamicKafkaConsumer.Services;
using KafkaCommon;
using KafkaCommon.Abstractions;
using KafkaCommon.Serialization;
using KafkaCommon.Services.Interfaces;
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
        KafkaEventsHandler, DeserializerInstances.Utf8Deserializer, DeserializerInstances.JsonDeserializer<SportInfoMessage>>(
        configuration,
        shouldSkipMessageProcessorIfAlreadyRegistered: true, // Processor is already registered right above and we don't want duplicated logic from exactly the same processor
        addKafkaConfigurationWithDefaultSectionName: false);

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