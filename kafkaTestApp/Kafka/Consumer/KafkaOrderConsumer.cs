using Confluent.Kafka;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.dlqType;

namespace kafkaTestApp.Kafka.Consumer;

public class KafkaOrderConsumer : KafkaConsumerBase<OrderAv>
{
    public string topic = "order-topic";
    public KafkaOrderConsumer(KafkaProducer<DlqType<OrderAv>> kafkaDlqProducer, IConsumer<Ignore, OrderAv> consumer) : base(kafkaDlqProducer,consumer)
    {
        this._topic = topic;
    }
    public override async Task ProcessMesage(OrderAv message)
    {
        Console.WriteLine($"Processing: {message.Id} - { message.Product} - { message.Price} - { message.Quantity }");
        await Task.Delay(1000);
        //throw new Exception("Erreur de traitement simulée");
    }
}

