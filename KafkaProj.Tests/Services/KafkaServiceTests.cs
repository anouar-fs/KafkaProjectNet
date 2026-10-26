using AutoFixture;
using Confluent.Kafka;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.Producer;
using kafkaTestApp.Mapper;
using kafkaTestApp.Services;
using Moq;

namespace KafkaProj.Tests.Services;

public class KafkaServiceTests
{
    public Fixture fixture;

    public KafkaServiceTests()
    {
        fixture = new Fixture();
    }

    [Fact]
    public async Task SendMessageAsync_ShouldMapOrderAndSendItToKafka()
    {
        var order = fixture.Create<OrderTest>();
        var expectedSchema = fixture.Create<OrderAv>();

        var mapperMock = new Mock<OrderToOrderAv>();
        var producerMock = new Mock<IKafkaProducer<OrderAv>>();

        mapperMock.Setup(m => m.toOrderAv(order))
            .Returns(expectedSchema);

        producerMock.Setup(m => m.SendMessageAsync(expectedSchema,"orderTest-topic"))
            .Returns(Task.CompletedTask);

        var service = new KafkaService(producerMock.Object,mapperMock.Object);

        await service.SendMessageAsync(order,"orderTest-topic");

        mapperMock.Verify(
            m => m.toOrderAv(order),
            Times.Once);

        producerMock.Verify(
            p => p.SendMessageAsync(
                expectedSchema,
                "orderTest-topic"),
            Times.Once);
    }

}

