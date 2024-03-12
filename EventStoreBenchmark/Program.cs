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
        private byte[] _serializedData128;
        private byte[] _serializedData256;
        private byte[] _serializedData512;
        private byte[] _serializedData1024;
        private byte[] _serializedData2048;
        private byte[] _serializedData4196;
        private byte[] _serializedData16384;
        private byte[] _serializedData65536;
        private byte[] _serializedData262144;
        private byte[] _serializedData33554432; // it's above 30mb

        private string CreateStringWithSpecificLength(int length)
        {
            var str = "";
            for (int i = 0; i < length; i++)
            {
                str += "a";
            }
            return str;
        }

        [GlobalSetup]
        public void Setup()
        {
            _client = CreateClient();
            _threadId = Guid.NewGuid();
            var ev = new MessageSent { Text = "" }; // ev weights 55 bytes
            ev.Text += "aaaaaaaaa"; // now, ev weights 64 bytes

            for (int index = 64; index < 33554433; index+=64)
            {
               
                switch (index)
                {
                    case 128:
                        _serializedData128 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 256:
                        _serializedData256 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 512:
                        _serializedData512 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 1024:
                        _serializedData1024 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 2048:
                        _serializedData2048 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 4196:
                        _serializedData4196 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 16384:
                        _serializedData16384 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 65536:
                        _serializedData65536 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 262144:
                        _serializedData262144 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;
                    case 33554432:
                        _serializedData33554432 = JsonSerializer.SerializeToUtf8Bytes(ev);
                        break;

                }
                ev.Text += CreateStringWithSpecificLength(64);
            }
        }
     

        [Benchmark]
        public async Task Append1024BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData1024);

        [Benchmark]
        public async Task Append4196BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData4196);

        [Benchmark]
        public async Task Append65536BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData65536);

        [Benchmark]
        public async Task Append262144BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData262144);

        [Benchmark]
        public async Task Append33554432BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData33554432);

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
