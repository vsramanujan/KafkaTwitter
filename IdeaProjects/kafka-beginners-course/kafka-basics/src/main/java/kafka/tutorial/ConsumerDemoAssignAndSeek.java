package kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignAndSeek { //No group ID, No subscribe : Read wherever we want to read from
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignAndSeek.class.getName());

        String topic = "first_topic";

        Properties properties = new Properties();
        //Check New Consumer Configs -> in kafka documentation

        // Step 1: Create Consumer Configs
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()); //Since producer serializes a string into bytes and sends it to Kafka, the consumer needs to do that opposite
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Takes 3 different values - earliest, latest, none | Earliest - from beginning | Latest - only new messages | none -  Will throw an error if there's no offsets being saved

        //Step 2: Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String,String> (properties);
        long offsetToReadFrom = 15L;
        //Step 3 !!!!!!!!!!!!!!!!! - Assign and Seek are mostly used to replay data or to fetch a specific message

        //Step 3.1 - Assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        consumer.assign(Arrays.asList(partitionToReadFrom));

        //Step 3.2 - Seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        //Change for  while(true) in Step 4
        int numberOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberOfMessagesReadSoFar = 0;

        //Step 4: Poll for new data
        while (keepOnReading) {
            //consumer.poll(100); // Poll is deprecated as poll now no longer takes a long but rather a duration - IMP!!
            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));

            //Step 4.1: Look at the records
            for(ConsumerRecord<String,String> record: records) { //Reads from beginning stuff partition by partition but new messages are read as they arrive
                numberOfMessagesReadSoFar +=  1;
                logger.info("\nKey"+ record.key() + "\nValue " + record.value() + "\nPartition: " + record.partition()+ "\nOffset: " + record.offset());
                if(numberOfMessagesReadSoFar >= numberOfMessagesToRead) {
                     keepOnReading = false;
                     break;
                }
            }
        }
        logger.info("Exiting the application");
    }
}
