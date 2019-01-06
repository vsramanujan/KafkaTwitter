package kafka.tutorial;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {
    public static void main(String[] args) {

        String bootstrapServers = "127.0.0.1:9092";

        //STEP 1 : Create producer properties
        // To find out what configuration to add, go to Kafka documentation and find producer config
        //Key serializer and value serializer basically help the producer to know what type of value you are sending to Kafka and how this should be serialized to bytes

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG , StringSerializer.class.getName()); // This is the old way
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //STEP 2 : Create the Producer
        //
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);

        //STEP 3 : Send Data - ASYNCHRONOUS! So if we don't give producer.flush() or producer.close() program will just end

        ProducerRecord<String,String> record = new ProducerRecord<String, String>("first_topic", "Hello World");
        producer.send(record);


        //STEP 4 : Flush and close
        producer.flush();
        producer.close();
    }
}
