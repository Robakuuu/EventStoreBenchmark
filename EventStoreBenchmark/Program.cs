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

        private byte[] _serializedData512;
        private byte[] _serializedData4196;
        private byte[] _serializedData65536;
        private byte[] _serializedData262144;
        private byte[] _serializedData1048576; // it's 1mb
        private byte[] _serializedData2097152;
        private byte[] _serializedData4194304;
        private byte[] _serializedData8388608;
        private byte[] _sampleHDVideoFrameBW;
        private byte[] _sampleHDVideoFrameCOLOR;
        private byte[] _sampleFullHDVideoFrameBW;
        private byte[] _sampleFullHDVideoFrameCOLOR;

        private string CreateStringWithSpecificLength(int length)
        {
            char[] chars = new char[length];
            Array.Fill(chars,'a');
            return new string(chars);
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
           _serializedData2097152 = GetSerializedEventWithSpecificSize(2097152);
            _serializedData4194304 = GetSerializedEventWithSpecificSize(4194304);
            _serializedData8388608 = GetSerializedEventWithSpecificSize(8388608);
            _sampleHDVideoFrameBW = new byte[1080 * 720 ]; // 1 channel, hd frame
            _sampleHDVideoFrameCOLOR = new byte[1080 * 720 * 3]; // 3 channel, hd frame
            _sampleFullHDVideoFrameBW = new byte[1920 * 1080 ]; // 1 channel, full hd frame
            _sampleFullHDVideoFrameCOLOR = new byte[1920 * 1080 * 3]; // 3 channel, full hd frame

        }

        private byte[] GetSerializedEventWithSpecificSize(int sizeInBytes)
        {
            var ev = new MessageSent { Text = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" }; // this ev weights 128 bytes;
            ev.Text += CreateStringWithSpecificLength(sizeInBytes - 128);
            return JsonSerializer.SerializeToUtf8Bytes(ev);
        }

       // [Benchmark]
        [InvocationCount(1)]
        public async Task Append512BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData512);
       // [Benchmark]
        [InvocationCount(1)]
         public async Task Append4196BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData4196);
        
       // [Benchmark]
        [InvocationCount(1)]
         public async Task Append65536BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData65536);
        
       // [Benchmark]
        [InvocationCount(1)]
         public async Task Append262144BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData262144);
        // [Benchmark]
         [InvocationCount(1)]
         public async Task Append1048576BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData1048576);
       // [Benchmark]
        [InvocationCount(1)]
        public async Task Append2097152BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData2097152);

        //[Benchmark]
        [InvocationCount(1)]
        public async Task Append4194304BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData4194304);

        //[Benchmark]
        [InvocationCount(1)]
        public async Task Append8388608BytesEvent() => await this.AppendSerializedEventToExistingStream(_serializedData8388608);

        [Benchmark]
        [InvocationCount(1)]
        public async Task AppendHDBWVideoFrameEvent() => await this.AppendSerializedEventToExistingStream(typeof(VideoFrameSent), _sampleHDVideoFrameBW);

        [Benchmark]
        [InvocationCount(1)]
        public async Task AppendHDColorVideoFrameEvent() => await this.AppendSerializedEventToExistingStream(typeof(VideoFrameSent), _sampleHDVideoFrameCOLOR);


        [Benchmark]
        [InvocationCount(1)]
        public async Task AppendFullHDBWVideoFrameEvent() => await this.AppendSerializedEventToExistingStream(typeof(VideoFrameSent), _sampleFullHDVideoFrameBW);


        [Benchmark]
        [InvocationCount(1)]
        public async Task AppendFullHDColorVideoFrameEvent() => await this.AppendSerializedEventToExistingStream(typeof(VideoFrameSent), _sampleFullHDVideoFrameCOLOR);




        public async Task AppendSerializedEventToExistingStream(Type eventType,byte[] data)
        {
            await _client.AppendToStreamAsync(
                $"Movie-{_threadId}",
                StreamState.Any, new[]
                {
                    new EventData(
                        Uuid.NewUuid(),
                        nameof(eventType),
                        data, null,"application/octet-stream")
                }

            );
        }
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
           BenchmarkRunner.Run<SendMessagesBenchmark>();
        }

    }
}
