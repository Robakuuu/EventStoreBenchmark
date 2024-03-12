using System.Text.Json;
using System;
using BenchmarkDotNet.Attributes;
using EventStore.Client;
using BenchmarkDotNet.Running;

namespace EventStoreBenchmark
{
    [JsonExporterAttribute.Full]
    public class SendMessagesBenchmark
    {
        private  string _topicId;
        private  EventStoreClient _client;
        private  EventData _eventData;
        private  EventData[] _1eventData;
        private  EventData[] _2eventData;
        private  EventData[] _5eventData;
        private  EventData[] _10eventData;



        [GlobalSetup]
        public void Setup()
        {
            _client = CreateClient();

        }
        [Benchmark]
        public async Task SendOneMessage() => this.SendMessages(_client, _topicId, _1eventData);


        public async Task SendMessages(EventStoreClient client, string topicId, EventData[] eventData)
        {
            var evt = new MessageSent { Text = "TestText" };
            await client.AppendToStreamAsync(
                $"Thread-{Guid.NewGuid()}",
                StreamState.Any, new EventData[]
                {
                    new EventData(
                        Uuid.NewUuid(),
                        nameof(MessageSent),
                        JsonSerializer.SerializeToUtf8Bytes(evt))
                }
            );
        }
        static EventStoreClient CreateClient()
        {
            const string connectionString = "esdb://admin:changeit@localhost:2113?tls=false&tlsVerifyCert=false";

            var settings = EventStoreClientSettings.Create(connectionString);

            var client = new EventStoreClient(settings);
            return client;
        }
    }

    internal class Program
    {
        static async Task Main(string[] args)
        {


              var summary = BenchmarkRunner.Run<SendMessagesBenchmark>();
        
        }





    }
}
