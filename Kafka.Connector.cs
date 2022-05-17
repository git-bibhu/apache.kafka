using Confluent.Kafka;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Kafka.Connector
{
    /// <summary>
    /// Kafka Connector class
    /// </summary>
    public class KafkaConnector
    {
        /// <summary>
        /// Consume generic type to respective topic using kafka
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic"></param>
        /// <returns></returns>
        public static T Subscribe<T>(string topic)
        {
            var conf = new ConsumerConfig
            {
                GroupId = "request-consumer",
                BootstrapServers = "localhost:9092",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using var c = new ConsumerBuilder<Ignore, T>(conf).Build();
            c.Subscribe(topic);

            // This is to capture Ctrl+C and use a cancellation token to get out of our while loop and close the consumer.
            var cts = new CancellationTokenSource();
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                cts.Cancel();
            };

            try
            {
                // Consume a message from the topic. Pass in a cancellation token so we can break out of our loop when Ctrl+C is pressed
                var cr = c.Consume(cts.Token);
                return cr.Message.Value;
            }
            catch (OperationCanceledException ex)
            {
                throw ex;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                c.Close();
            }
        }

        /// <summary>
        /// Publish generic type to respective topic using kafka
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="topic"></param>
        /// <param name="payload"></param>
        /// <returns></returns>
        public static async Task<T> Publish<T>(string topic, T payload)
        {
            try
            {
                var config = new ProducerConfig
                {
                    BootstrapServers = "localhost:9092"
                };

                // Create a producer that can be used to send messages to kafka that have no key and a value of type T 
                using var p = new ProducerBuilder<Null, T>(config).Build();

                // Construct the message to send T type
                var message = new Message<Null, T>
                {
                    Value = payload
                };

                // Send the message to the topic in Kafka                
                var dr = await p.ProduceAsync(topic, message);
                return dr.Value;
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }
    }
}
