namespace EventStoreBenchmark;

public record VideoFrameSent
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public required byte[] Data { get; set; }
}