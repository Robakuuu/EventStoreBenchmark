using System.Text.Json;
using System;
using BenchmarkDotNet.Attributes;
using EventStore.Client;
using BenchmarkDotNet.Running;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Exporters;
using System.Reflection.Metadata.Ecma335;

namespace EventStoreBenchmark
{
  
    [JsonExporterAttribute.Full]
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

        public byte[] Get262144bytesEvent()
        {

            _client = CreateClient();
            _threadId = Guid.NewGuid();
            var ev = new MessageSent { Text = "aaaaaaaaa" }; // this ev weights 64 bytes;
         
            ev.Text += CreateStringWithSpecificLength(262144-64);
            return JsonSerializer.SerializeToUtf8Bytes(ev);
        }

        public byte[] Get4196bytesEvent()
        {

            _client = CreateClient();
            _threadId = Guid.NewGuid();
            var ev = new MessageSent { Text = "aaaaaaaaa" }; // this ev weights 64 bytes;

            ev.Text += CreateStringWithSpecificLength(4196-64);
            return JsonSerializer.SerializeToUtf8Bytes(ev);
        }

       // [Benchmark]
       // [ArgumentsSource(nameof(Get262144bytesEvent))]
       // [InvocationCount(10)]
       // public async Task Append262144BytesEvent(byte[] serializedEvent) => await this.AppendSerializedEventToExistingStream(serializedEvent);

       [Benchmark]
       [ArgumentsSource(nameof(Get4196bytesEvent))]
        public async Task Append4196BytesEvent(byte[] serializedEvent) => await this.AppendSerializedEventToExistingStream(serializedEvent);
       //
       //[Benchmark]
       //public async Task Append65536BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData65536);
       //
       //[Benchmark]
       //[InvocationCount(10)]
       //public async Task Append262144BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData262144);
       //
       //[Benchmark]
       //[InvocationCount(1)]
       //public async Task Append33554432BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData33554432);

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
