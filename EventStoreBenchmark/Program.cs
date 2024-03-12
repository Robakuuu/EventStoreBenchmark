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
        private byte[] _serializedData1048576; // it's 1mb

        private string CreateStringWithSpecificLength(int length)
        {
            var str = "";
            for (int i = 0; i < length; i+=8)
            {
                str += "aaaaaaaa";
            }
            return str;
        }


        [GlobalSetup]
        public void Setup()
        {

            _client = CreateClient();
            _threadId = Guid.NewGuid();

            _serializedData512 = GetSerializedEventWithSpecificSize(512);
            _serializedData4196 = GetSerializedEventWithSpecificSize(4196);
            _serializedData65536 = GetSerializedEventWithSpecificSize(65536);
            _serializedData262144 = GetSerializedEventWithSpecificSize(262144);
            _serializedData1048576 = GetSerializedEventWithSpecificSize(1048576);
        }

        private byte[] GetSerializedEventWithSpecificSize(int sizeInBytes)
        {
            var ev = new MessageSent { Text = "aaaaaaaaa" }; // this ev weights 64 bytes;
            ev.Text += CreateStringWithSpecificLength(sizeInBytes - 64);
            return JsonSerializer.SerializeToUtf8Bytes(ev);
        }

     
       [Benchmark]
       [InvocationCount(1)]
        public async Task Append4196BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData4196);
       
       [Benchmark]
       [InvocationCount(1)]
        public async Task Append65536BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData65536);

       [Benchmark]
       [InvocationCount(1)]
        public async Task Append262144BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData262144);
        [Benchmark]
        [InvocationCount(1)]
        public async Task Append1048576BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData1048576);


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
