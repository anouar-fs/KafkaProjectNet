namespace KafkaProj.Tests.Mappers;
using AutoFixture;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Mapper;
using Xunit;

public class OrderToOrderAvTests
{
    public Fixture fixture;

    public OrderToOrderAvTests()
    {
        fixture = new Fixture();
    }

    [Fact]
    public void ToOrderAv_ShouldMapAllProperties()
    {
        var order = fixture.Create<OrderTest>();

        var orderToOrderAv = new OrderToOrderAv();

        OrderAv orderAv = orderToOrderAv.toOrderAv(order);

        orderAv.Id.Equals(order.Id);
        orderAv.Price.Equals(order.Price);
        orderAv.Quantity.Equals(order.Quantity);
        orderAv.Product.Equals(order.Product);

    }
}

