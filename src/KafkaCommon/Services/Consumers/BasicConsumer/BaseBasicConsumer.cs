namespace KafkaCommon.Services.Consumers.BasicConsumer;

using System.Text.Json;
using Confluent.Kafka;
using Abstractions;
using Configuration;
using Extensions;
using Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public abstract class BaseBasicConsumer<TKey, TValue> where TValue : class
{
        private readonly IConsumerEventsHandler _consumerEventsHandler;
        private readonly ILogger _logger;
        private readonly JsonValueSerializer<TValue> _serializer;
        private readonly KafkaConfiguration _readonlyConsumerConfig;

        public BaseBasicConsumer(IOptionsMonitor<KafkaConfiguration> config, IConsumerEventsHandler consumerEventsHandler, ILogger logger, JsonValueSerializer<TValue> serializer)
        {
            if (!config.CurrentValue.Consumers.ContainsKey(ConsumerConfigurationName))
                throw new ArgumentException($"Could not construct dynamic kafka consumer since configuration is lacking consumer section with name: {ConsumerConfigurationName}");
            _consumerEventsHandler = consumerEventsHandler;
            _logger = logger;
            _serializer = serializer;

            Config = config.CurrentValue;
            ConsumerConfig = Config.Consumers[ConsumerConfigurationName];
            ConfigureConsumerTopics();
            MergeConsumerSettings();
            
            NotCommittedMessagesCount = 0;
            _readonlyConsumerConfig = Config;
        }
        protected virtual int ConsumeTimeoutMs => 100;
        
        protected int NotCommittedMessagesCount { get; set; }

        protected KafkaConfiguration Config { get; }
        
        protected TopicConfiguration ConsumerConfig { get; }

        protected abstract string ConsumerConfigurationName { get; }

        protected IConsumer<TKey, TValue> Consumer { get; set; }

        //Override if needed for child classes
        protected virtual bool ShouldProcessMessage(ConsumeResult<TKey, TValue> consumeResult) => true;

        /// <summary>
        /// Can be overriden and modified by child classes in case they have to wait for some resources to load before starting to consume
        /// </summary>
        protected virtual bool ShouldStartConsuming() => true;

        protected virtual void BuildConsumer() 
            => Consumer = BasicConsumerBuilder.CreateConsumer<TKey, TValue>(new ConsumerConfig(ConsumerConfig.Settings), _consumerEventsHandler, _serializer);

        public List<TopicPartition> PartitionsAssigned() => Consumer.Assignment;

        public KafkaConfiguration CurrentConfiguration() => _readonlyConsumerConfig;
        
        public  bool IsConsumerSet()
        {
            try
            {
                var _ = Consumer.Assignment;
                return true;
            }
            catch (ObjectDisposedException e)
            {
                return false;
            }
            catch (Exception e)
            {
                _logger.LogError(e, $"An error occured during {nameof(IsConsumerSet)}");
                return false;
            }
        }

        public void Subscribe(CancellationToken cancellationToken)
        {
            var isSubscribed = false;
            do
            {
                try
                {
                    var beforeSub = JsonSerializer.Serialize(Consumer);
                    Consumer.Subscribe(ConsumerConfig.Topics);
                    var afterSb = JsonSerializer.Serialize(Consumer);
                    isSubscribed = Consumer.Subscription.Any();
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to subscribe to topics: {string.Join(" ,", ConsumerConfig.Topics)}");
                }
            } while (!isSubscribed && !cancellationToken.IsCancellationRequested);
        }

        public void Unsubscribe(CancellationToken cancellationToken)
        {
            const int secondsToUnsubscribe = 120;
            var unsubscribeUntil = DateTime.Now.AddSeconds(secondsToUnsubscribe);
            var isSuccess = false;
            do
            {
                try
                {
                    Consumer.Unsubscribe();
                    isSuccess = true;
                }
                catch (ObjectDisposedException e)
                {
                    isSuccess = true;
                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, $"Failed to unsubscribe from topics: {string.Join(" ,", ConsumerConfig.Topics)}");
                }

                Thread.Sleep(25);
            } while (!isSuccess && !cancellationToken.IsCancellationRequested && unsubscribeUntil >= DateTime.Now);
        }

        protected void DisposeCurrentConsumer()
        {
            try 
            {
                Consumer?.Close();
                Consumer?.Dispose();
            } catch (ObjectDisposedException)  { /* suppress */ }
        }

        protected virtual bool ManuallyHandleOffsetCommit() => false;

        public void ManuallyCommitOffset(ConsumeResult<TKey, TValue> consumeResult) => Consumer.Commit(consumeResult);

        public void ManuallyCommitOffset(IEnumerable<TopicPartitionOffset> offsets) => Consumer.Commit(offsets);

        // Executes CommitOffset but suppresses any potential exception
        protected void SafeCommitOffset()
        {
            try
            {
                CommitOffset();
            }
            catch (Exception){ /* suppress */ }
        }

        protected void CommitOffset()
        {
            if (ManuallyHandleOffsetCommit())
                return;
            
            if (NotCommittedMessagesCount > 0)
                Consumer.Commit();
        }

        protected void HandleNotCommittedOffsets()
        {
            NotCommittedMessagesCount++;
            if (ManuallyHandleOffsetCommit())
                return;
            
            if (NotCommittedMessagesCount >= ConsumerConfig.MaxNotCommittedMessages)
            {
                Consumer.Commit();
                NotCommittedMessagesCount = 0;
            }
        }

        /// <summary>
        /// Try to set Config.Topics to the topics from the configuration if any
        /// </summary>
        /// <exception cref="ArgumentException"></exception>
        private void ConfigureConsumerTopics()
        {
            if (ConsumerConfig.Topics.IsNullOrEmpty())
            {
                if (!Config.BaseConfig.Topics.IsNullOrEmpty())
                {
                    ConsumerConfig.Topics = Config.BaseConfig.Topics;
                }
                else
                {
                    throw new ArgumentException($"No topic configured for consumer {ConsumerConfigurationName}");
                }
            }
        }

        /// <summary>
        /// Merge base settings from config with consumer settings
        /// </summary>
        private void MergeConsumerSettings()
        {
            var baseSettings = Config.BaseConfig?.BaseSettings;
            if (baseSettings != null && baseSettings.Any())
            {
                foreach (var setting in baseSettings)
                {
                    if (!ConsumerConfig.Settings.ContainsKey(setting.Key))
                        ConsumerConfig.Settings.Add(setting.Key, setting.Value);
                }
            }
        }
}