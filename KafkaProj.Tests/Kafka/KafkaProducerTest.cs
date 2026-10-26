
using Confluent.Kafka;
using Moq;

namespace KafkaProj.Tests.Kafka;

public class KafkaProducerTest
{
    [Fact]
    public async Task SendMessageAsync_ShouldSendCorrectMessage()
    {
        var kafkaProdcuerMock = new Mock<IProducer<Null, string>>();

        kafkaProdcuerMock
           .Setup(p => p.ProduceAsync(
               "orderTest-topic",
               It.Is<Message<Null, string>>(
                   m => m.Value == "hello"),
               It.IsAny<CancellationToken>()))
           .ReturnsAsync(
               (DeliveryResult<Null, string>)null!);

        var producer = new KafkaProducer<string>(kafkaProdcuerMock.Object);

        await producer.SendMessageAsync("hello","orderTest-topic");

        kafkaProdcuerMock.Verify( p => p.ProduceAsync(
                "orderTest-topic",
                It.Is<Message<Null,string>>(m => m.Value == "hello"),
                It.IsAny<CancellationToken>()),
                Times.Once
            );
    }
}

