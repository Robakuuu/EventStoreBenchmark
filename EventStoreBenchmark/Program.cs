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
        private byte[] _serializedData64;
        private byte[] _serializedData128;
        private byte[] _serializedData256;
        private byte[] _serializedData512;
        private byte[] _serializedData1024;
        private byte[] _serializedData2048;
        private byte[] _serializedData4196;
        private byte[] _serializedData16384;
        private byte[] _serializedData65536;
        private byte[] _serializedData262144;


        [GlobalSetup]
        public void Setup()
        {
            _client = CreateClient();
            _threadId = Guid.NewGuid();
            var ev = new MessageSent { Text = "" };

            for (int index = 1; index < 262145; index++)
            {
                ev.Text += "a";
                var serializedEvent = JsonSerializer.SerializeToUtf8Bytes(ev);
                switch (serializedEvent.Length)
                {
                    case 64:
                        _serializedData64 = serializedEvent;
                        break;
                    case 128:
                        _serializedData128 = serializedEvent;
                        break;
                    case 256:
                        _serializedData256 = serializedEvent;
                        break;
                    case 512:
                        _serializedData512 = serializedEvent;
                        break;
                    case 1024:
                        _serializedData1024 = serializedEvent;
                        break;
                    case 2048:
                        _serializedData2048 = serializedEvent;
                        break;
                    case 4196:
                        _serializedData4196 = serializedEvent;
                        break;
                    case 16384:
                        _serializedData16384 = serializedEvent;
                        break;
                    case 65536:
                        _serializedData65536 = serializedEvent;
                        break;
                    case 262144:
                        _serializedData262144 = serializedEvent;
                        break;
                }
            }
        }
        [Benchmark]
        public async Task Append64BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData64);

        [Benchmark]
        public async Task Append1024BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData1024);

        [Benchmark]
        public async Task Append262144BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData262144);

        public async Task AppendSerializedEventToExistingStream(byte[] data)
        {
            await _client.AppendToStreamAsync(
                $"Thread-{_threadId}",
                StreamState.Any, new[]
                {
                    new EventData(
                        Uuid.NewUuid(),
                        nameof(MessageSent),
                        data)
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
