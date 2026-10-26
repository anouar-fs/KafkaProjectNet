namespace kafkaTestApp.Kafka.Consumer;
public interface IKafkaConsumerBase<TPayload>
{
    public Task ConsumeMessages(CancellationToken cancellationToken);
    public Task ProcessMessageWithRetries(TPayload message);
}

