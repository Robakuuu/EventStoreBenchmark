using System.Text.Json;
using System;
using BenchmarkDotNet.Attributes;
using EventStore.Client;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;

namespace EventStoreBenchmark
{
  
    [JsonExporterAttribute.Full]
    [MaxIterationCount(15)]
    public class SendMessagesBenchmark
    {
        private  string _topicId;
        private  EventStoreClient _client;
        private  EventData _eventData;
        private Guid _threadId;
        private byte[] _serializedEvent;

   

        [GlobalSetup]
        public void Setup()
        {
            _client = CreateClient();
            _threadId = Guid.NewGuid();

        }
        [Benchmark]
        public void SendOneMessage() => this.SendMessages();

      

        public void SendMessages()
        {
            var evt = new MessageSent { Text = "TestText" };
             _client.AppendToStreamAsync(
                $"Thread-{_threadId}",
                StreamState.Any, new []
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
        static void Main(string[] args)
        {


              var summary = BenchmarkRunner.Run<SendMessagesBenchmark>();
        
        }





    }
}
