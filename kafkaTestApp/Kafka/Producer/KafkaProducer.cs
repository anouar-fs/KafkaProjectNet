using Confluent.Kafka;

public class KafkaProducer<TShema>
{
    public readonly IProducer<Null, TShema> _producer;
    public KafkaProducer(IProducer<Null, TShema> producer)
    {
        this._producer = producer;
    }

    public async Task SendMessageAsync(TShema payload,string topic)
    {

        await this._producer.ProduceAsync(
            topic,
            new Message<Null, TShema>
            {
                Value = payload
            });
    }
}