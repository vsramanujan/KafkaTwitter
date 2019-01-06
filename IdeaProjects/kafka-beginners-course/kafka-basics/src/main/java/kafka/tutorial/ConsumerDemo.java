package kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo  {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemo.class.getName());

        String groupId = "My Fourth Application";
        String topic = "first_topic";

        Properties properties = new Properties();
        //Check New Consumer Configs -> in kafka documentation

        // Step 1: Create Consumer Configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Since producer serializes a string into bytes and sends it to Kafka, the consumer needs to do that opposite
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Takes 3 different values - earliest, latest, none | Earliest - from beginning | Latest - only new messages | none -  Will throw an error if there's no offsets being saved

        //Step 2: Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String> (properties);

        //Step 3: Subscribe consumer to topic(s)
        consumer.subscribe(Collections.singleton(topic)); // Collections.singleton - basically says we are subscribing to only one topic
        //IMPORTANT: To subscribe to Multiple Topics -
        // consumer.subscribe(Arrays.asList("first_topic", "second_topic", "third_topic"));

        //Step 4: Poll for new data
        while (true) {
            //consumer.poll(100); // Poll is deprecated as poll now no longer takes a long but rather a duration - IMP!!
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            //Step 4.1: Look at the records
            for(ConsumerRecord<String,String> record: records) { //Reads from beginning stuff partition by partition but new messages are read as they arrive
                logger.info("\nKey"+ record.key() + "\nValue " + record.value());
                logger.info("\nPartition: " + record.partition());
            }
        }
    }
}
