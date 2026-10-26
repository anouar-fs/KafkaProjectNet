namespace kafkaTestApp.Kafka.Producer;

public interface IKafkaProducer<TShema>
{
    public Task SendMessageAsync(TShema payload, string topic);
}

