using Confluent.Kafka;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.dlqType;
using kafkaTestApp.Kafka.Producer;

namespace kafkaTestApp.Kafka.Consumer;

public class KafkaOrderConsumer : KafkaConsumerBase<OrderAv>
{
    public string topic = "order-topic";
    public KafkaOrderConsumer(IKafkaProducer<DlqType<OrderAv>> kafkaDlqProducer, IConsumer<Ignore, OrderAv> consumer) : base(kafkaDlqProducer,consumer)
    {
        this._topic = topic;
    }
    public override async Task ProcessMesage(OrderAv message)
    {
        Console.WriteLine($"This is the messaged that get processed : *ID  : {message.Id} - *Product : { message.Product} - *Price : { message.Price} - *Quantity : { message.Quantity }");
        await Task.Delay(1000);
        //throw new Exception("Erreur de traitement simulée");
    }
}
