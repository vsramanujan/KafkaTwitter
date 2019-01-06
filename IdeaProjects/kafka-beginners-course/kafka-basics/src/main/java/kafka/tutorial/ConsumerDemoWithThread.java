package kafka.tutorial;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    public static void main(String[] args) {
         new ConsumerDemoWithThread().run();
    }

    public ConsumerDemoWithThread() {

    }

    public void run() {
        Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThread .class.getName());
        String groupId = "My Sixth Application";
        String topic = "first_topic";
        String bootstrapServer = "localhost:9092";

        //Latch for dealing with multiple threads
        CountDownLatch latch = new CountDownLatch(1);

        //Create the consumer runnable
        logger.info("Creating the consumer thread");
        Runnable myConsumerRunnable = new ConsumerRunnable(topic,bootstrapServer,groupId,latch);

        //Start the thread
        Thread myThread = new Thread(myConsumerRunnable);
        myThread.start();

        //Add a shutdown hook
        Runtime.getRuntime().addShutdownHook( new Thread( () -> {
            logger.info("Caught shutdown hook");
             ((ConsumerRunnable) myConsumerRunnable).shutdown();
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    logger.info("Application has exited");
                })


        );

        try {
            latch.await(); //Waits all the way until the application is over
        } catch (InterruptedException e) {
            logger.info("Application got interrupted",e);
        } finally {
            logger.info("Application is closing");
        }
    }



    public class ConsumerRunnable implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
        private CountDownLatch latch; //CountDownLatch - something you do in Java to deal with concurrency
        private KafkaConsumer<String,String> consumer;

        public  ConsumerRunnable(String topic, String bootstrapServer, String groupId, CountDownLatch latch) {  // This latch is going to be able to shutdown our application correctly
            this.latch = latch;

            Properties properties = new Properties();

            // Step 1: Create Consumer Configs
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

            //Step 2: Create consumer
            consumer = new KafkaConsumer<String,String> (properties);

            //Step 3: Subscribe consumer to topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {

            try {
                //Step 4: Poll for new data

                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    //Step 4.1: Look at the records
                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("\nKey" + record.key() + "\nValue " + record.value());
                        logger.info("\nPartition: " + record.partition());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal!");
            } finally {
                consumer.close();
                latch.countDown(); //tell the main code that we're done with the consumer
            }
        }

        public void shutdown() { //Going to shutdown our thread
            consumer.wakeup(); // This is a special method to interrupt consumer.poll and will throw WakeUpException
        }
    }
}
