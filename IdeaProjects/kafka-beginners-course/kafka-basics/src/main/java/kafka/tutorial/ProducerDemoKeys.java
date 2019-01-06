package kafka.tutorial;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        //Simply - create a logger for my class
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);


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


        for(int i=0;i<10;i++) { //If you do this, messages won't get displayed on the terminal in order because it is being sent to three different partitions and we haven't sent any keys

           String topic = "first_topic";
           String value = "Hello World " + Integer.toString(i);
           String key =  "id_" + Integer.toString(i);

           logger.info("Key," + key); // Same key always goes to same partition
           //ID0 -> Partition1
            //ID1 -> 0
            //2
            //0
            //2
            //2
            //0
            //2
            //1
            //2


            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic,key, value );
            producer.send(record, new Callback() { // To get the callback, type new Call and see the autocomplete
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //This executes every time a record is successfully sent or if an exception is thrown
                    //Now the recordMetadata has a bunch of properties that give you stuff like timestamp, topic etc
                    //So it will be useful to log it
                    if (e == null) {
                        //Record was successfully sent
                        logger.info("\nReceived new metadata: \n" +
                                "Topic = " + recordMetadata.topic() + "\n" +
                                "Partition = " + recordMetadata.partition() + "\n" +
                                "Offset = " + recordMetadata.offset() + "\n" +
                                "Timestamp = " + recordMetadata.timestamp() + "\n");
                    } else {

                        logger.error("Error while producing ", e);
                    }
                }
            }).get(); //Bad practice, but makes the send method synchronous
        }


        //STEP 4 : Flush and close
        producer.flush();
        producer.close();
    }
}
