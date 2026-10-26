using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.Producer;
using kafkaTestApp.Mapper;

namespace kafkaTestApp.Services;

public class KafkaService
{
    public IKafkaProducer<OrderAv> _producer;
    public OrderToOrderAv _mapper;
    public KafkaService(IKafkaProducer<OrderAv> producer, OrderToOrderAv mapper)
    {
        this._producer = producer;
        this._mapper = mapper;
    }

    public async Task SendMessageAsync(OrderTest order,string topic)
    {
        var orderAv = this._mapper.toOrderAv(order);

        await this._producer.SendMessageAsync(orderAv,topic);
    }
}
