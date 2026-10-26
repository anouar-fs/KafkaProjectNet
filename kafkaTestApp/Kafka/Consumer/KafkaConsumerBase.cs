using Confluent.Kafka;
using kafkaTestApp.Kafka.dlqType;
using kafkaTestApp.Kafka.Producer;

namespace kafkaTestApp.Kafka.Consumer;
public abstract class KafkaConsumerBase<TPayload>: BackgroundService,IKafkaConsumerBase<TPayload>
{
    private readonly IKafkaProducer<DlqType<TPayload>> _kafkaDlqProducer;
    private readonly IConsumer<Ignore, TPayload> _consumer;

    public string _topic { get; set; } = string.Empty;

    public KafkaConsumerBase(IKafkaProducer<DlqType<TPayload>> kafkaDlqProducer, IConsumer<Ignore, TPayload> consumer) {
        _kafkaDlqProducer = kafkaDlqProducer;
        _consumer = consumer;
    }

    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        //return Task.Run(()=> ConsumeMessages(stoppingToken),stoppingToken);
        return ConsumeMessages(stoppingToken);
    }

    public async Task ConsumeMessages(CancellationToken cancellationToken) {
        _consumer.Subscribe(_topic);

        try
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                var result = _consumer.Consume(cancellationToken);
                try
                {
                    Console.WriteLine($"Message received: {result.Message.Value}");

                    await ProcessMessageWithRetries(result.Message.Value);

                    _consumer.Commit(result);
                }
                catch (Exception ex) {
                    Console.WriteLine($"Message failed after retries: {ex.Message}");
                    try
                    {
                        var dlqMessage = new DlqType<TPayload>
                        {
                            OriginalMessage = result.Message.Value,
                            Error = ex.Message,
                            FailedAt = DateTime.UtcNow
                        };
                        await this._kafkaDlqProducer.SendMessageAsync(dlqMessage,"dlq_topic");
                        _consumer.Commit(result);
                        Console.WriteLine("Messages sent to DLQ and offset commited");
                    }
                    catch(Exception dlqException) {
                        Console.WriteLine($"Failed to send to DLQ: {dlqException.Message}");
                    }
                }
            }
        }
        catch (OperationCanceledException)
        {
            Console.WriteLine("Consumer stopped !");
        }
        finally
        {
            _consumer.Close();
        }

    }

    public async Task ProcessMessageWithRetries(TPayload message)
    {
        int maxRetries = 3;
        for (int i = 0; i < maxRetries; i++) {
            try
            {
                await ProcessMesage(message);

                Console.WriteLine("Message traité (done)");
                return;
            }
            catch (Exception exp)
            {
                Console.WriteLine("Retry number "+ (i+1) + " " + exp.Message);
                if (i == maxRetries-1) throw;
                int delay = 1000 * (int)Math.Pow(2, i);
                await Task.Delay(delay);
            }
        }
    }
    public abstract Task ProcessMesage(TPayload message);

}