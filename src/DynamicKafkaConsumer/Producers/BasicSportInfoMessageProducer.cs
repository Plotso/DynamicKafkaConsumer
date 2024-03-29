﻿namespace DynamicKafkaConsumer.Producers;

using Contracts;
using KafkaCommon.Configuration;
using KafkaCommon.Serialization;
using KafkaCommon.Services.Producers;
using Microsoft.Extensions.Options;

public class BasicSportInfoMessageProducer : BasicProducer<string, SportInfoMessage>
{
    public BasicSportInfoMessageProducer(IOptionsMonitor<KafkaConfiguration> kafkaConfiguration, JsonValueSerializer<SportInfoMessage> serializer) 
        : base(kafkaConfiguration, serializer)
    {
    }

    protected override string ConfigurationSectionName => nameof(BasicSportInfoMessageProducer);
}