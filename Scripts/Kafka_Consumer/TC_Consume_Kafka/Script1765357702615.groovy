import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import java.time.Duration

Properties props = new Properties()
props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
props.put(ConsumerConfig.GROUP_ID_CONFIG, "katalon-consumer-group")
props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)

String topicName = "my-topic-2"
consumer.subscribe(Arrays.asList(topicName))

println("=== Kafka Consumer Started ===")
println("Subscribed to: " + topicName)

try {

    while (true) {

        def records = consumer.poll(Duration.ofMillis(5000))

        records.each { record ->
            println("------------- Message Received -------------")
            println("Topic     : ${record.topic()}")
            println("Value     : ${record.value()}")
            println("--------------------------------------------")
        }
    }
}
finally {
    consumer.close()
    println("=== Kafka Consumer Closed ===")
}
