package kafka.tutorial2;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer  {

    private final Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());
    private String consumerKey = "s72cFbIVSfxckGVsRNyIEJf4d";
    private String consumerSecret = "DLfMLZn4fMmn3iuzGD3CgGCmQQBqj5NELdu6X0mkpG2MN7fTqy";
    private String token= "164330288-D4KuAzuG8Fd1Mj3wZaw3qNEPz7nGiep1uddGoyEl";
    private String secret = "6hJCKJ2z9eHikJvqxSaXm9LlkzxN51AAYayUEoFkXVBqq";

     List<String> terms = Lists.newArrayList("paypal","india","bitcoin");

    public TwitterProducer() {}


    public static void main(String[] args) {

        new TwitterProducer().run();
    }

    public void run() {


        logger.info("Setup");
        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);

        // Step 1 : Create a twitter client
        Client client = createTwitterClient(msgQueue);

        // Attempts to establish a connection.
        client.connect();

        // Step 2 : Create a kafka producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        //Step 3 : Loop to send tweets to kafka
        while (!client.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
                producer.send(new ProducerRecord<>("twitter_tweets", null, msg), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                         if(e != null) {
                             logger.error("Something bad happened",e); ;
                         }
                    }
                });
            }
        }
        logger.info("End application");
    }

    public Client  createTwitterClient(BlockingQueue<String> msgQueue) {

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        //  List<Long> followings = Lists.newArrayList(1234L, 566788L); // follow either people
        //List<String> terms = Lists.newArrayList("bitcoin"); // or terms ; here we basically follow terms
        //hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(consumerKey, consumerSecret, token, secret);


        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue));
                //.eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }

    public KafkaProducer<String,String> createKafkaProducer() {
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
        return producer;
    }

}
