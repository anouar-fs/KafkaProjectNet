
using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using kafkaTestApp.Kafka.Avro.Generated.KafkaApp;
using kafkaTestApp.Kafka.Consumer;
using kafkaTestApp.Kafka.dlqType;
using kafkaTestApp.Kafka.Producer;
using kafkaTestApp.Mapper;
using kafkaTestApp.Services;

var produceconfig = new ProducerConfig
{
    BootstrapServers = "broker:29092",
    EnableIdempotence = true,
    Acks = Acks.All,
    LingerMs = 50
};

var consumerconfig = new ConsumerConfig
{
    BootstrapServers = "broker:29092",
    GroupId = "orders-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

var schemaRegestry = new CachedSchemaRegistryClient(
    new SchemaRegistryConfig
    {
        Url = "http://schema-registry:8081"
    });


var builder = WebApplication.CreateBuilder(args);

// Add services to the container.

builder.Services.AddControllers();
// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddSingleton<IKafkaProducer<OrderAv>,KafkaProducer<OrderAv>>();
builder.Services.AddSingleton<IKafkaProducer<DlqType<OrderAv>>,KafkaProducer<DlqType<OrderAv>>>();
builder.Services.AddTransient<OrderToOrderAv>();
builder.Services.AddScoped<KafkaService>();

builder.Services.AddSingleton<IProducer<Null, OrderAv>>(_ => new ProducerBuilder<Null, OrderAv>(produceconfig)
            .SetValueSerializer(new AvroSerializer<OrderAv>(schemaRegestry))
            .Build());
builder.Services.AddSingleton<IProducer<Null, DlqType<OrderAv>>>(_ => new ProducerBuilder<Null, DlqType<OrderAv>>(produceconfig)
            .SetValueSerializer(new AvroSerializer<DlqType<OrderAv>>(schemaRegestry))
            .Build());
builder.Services.AddSingleton<IConsumer<Ignore, OrderAv>>( _ => new ConsumerBuilder<Ignore, OrderAv>(consumerconfig)
            .SetValueDeserializer(new AvroDeserializer<OrderAv>(schemaRegestry).AsSyncOverAsync())
            .Build());


builder.Services.AddHostedService<KafkaOrderConsumer>();

var app = builder.Build();
// Add swagger
app.UseSwagger();

app.UseSwaggerUI();

// Configure the HTTP request pipeline.
if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();

app.UseAuthorization();

app.MapControllers();

app.Run();