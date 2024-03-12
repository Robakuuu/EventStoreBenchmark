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
            _topicId = Guid.NewGuid().ToString();
            string line = "";

            _client = CreateClient();


            line = Guid.NewGuid().ToString();

            var evt = new MessageSent { Text = line };

            _eventData = new EventData(
                Uuid.NewUuid(),
                nameof(MessageSent),
                JsonSerializer.SerializeToUtf8Bytes(evt)
            );

            List<EventData> data = new List<EventData>();
            for (int index = 1; index <= 10; index++)
            {
                data.Add(_eventData);
                if (data.Count==1) _1eventData = data.ToArray();
                if (data.Count==2) _2eventData = data.ToArray();
                if (data.Count==5) _5eventData = data.ToArray();
                if (data.Count==10) _10eventData = data.ToArray();

            }
        }
        [Benchmark]
        public async Task SendOneMessage() => this.SendMessages(_client, _topicId, _1eventData);

       // [Benchmark]
      //  public async Task SendTwoMessage() => this.SendMessages(_client, _topicId, _2eventData);

      //  [Benchmark]
      //  public async Task Send5Message() => this.SendMessages(_client, _topicId, _5eventData);

     //   [Benchmark]
     //   public async Task Send10Message() => this.SendMessages(_client, _topicId, _10eventData);


        public async Task SendMessages(EventStoreClient client, string topicId, EventData[] eventData)
        {
            await client.AppendToStreamAsync(
                $"Thread-{topicId}",
                StreamState.Any,
                eventData
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
