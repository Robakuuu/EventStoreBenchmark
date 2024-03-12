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
    [InvocationCount(100)]
    public class SendMessagesBenchmark
    {
        private  string _topicId;
        private  EventStoreClient _client;
        private  EventData _eventData;
        private Guid _threadId;
        private byte[] _serializedEvent;
        private MessageSent _evt;
        private byte[] _serializedData;


        [GlobalSetup]
        public void Setup()
        {
            _client = CreateClient();
            _threadId = Guid.NewGuid();
            _evt = new MessageSent { Text = "TestText" };
            _serializedData= JsonSerializer.SerializeToUtf8Bytes(_evt);
        }
        [Benchmark()]
        public async Task SendOneMessage() => await this.SendMessages();



        public async Task SendMessages()
        {
            await _client.AppendToStreamAsync(
                $"Thread-{_threadId}",
                StreamState.Any, new[]
                {
                    new EventData(
                        Uuid.NewUuid(),
                        nameof(MessageSent),
                        _serializedData)
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
