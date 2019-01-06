package com.github.rveeravallisevil.kafka.tutorial3;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ElasticSearchConsumerIdempotentManualCommitWithBatching {

    private static JsonParser jsonParser = new JsonParser();

    public static RestHighLevelClient createClient() {
        String hostname= "kafka-learning-testi-4873898664.eu-west-1.bonsaisearch.net";
        String username= "zf107fyhzx";
        String password= "hfcdheyrcd";

        // Since we are doing this on Bonsai/on the cloud
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));

        //Create a HTTP Request
        RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,443,"https")).setHttpClientConfigCallback((new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider); // Apply the Credentials to it
            }
        }));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }


    public static KafkaConsumer<String,String> createConsumer(String topic) {
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        //Create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); //disable auto-commit of offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"100");

        //Create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe to topic
        consumer.subscribe(Arrays.asList(topic));

        return consumer;
    }

    private static String extractIDFromString(String tweetJson) {
        // We use the gson library
        return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").getAsString();
    }


    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumerIdempotentManualCommitWithBatching.class.getName());
        RestHighLevelClient client = createClient();

       // String jsonString = "{ \"foo\" : \"bar\"}";

        //Step 1 : Create an index request
//        IndexRequest indexRequest = new IndexRequest("twitter", "tweets").source(jsonString, XContentType.JSON);

        //Step 2: To run it, we need a client - so we create an IndexResponse object
//        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
//        String id = indexResponse.getId();
//        logger.info(id);


        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordCount = records.count();
            logger.info("Received " + recordCount + "count");
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record : records) { //Reads from beginning stuff partition by partition but new messages are read as they arrive
                //Where we insert data into ElasticSearch

                // 2 stratergies to creating IDs
                // 1. Kafka Generic ID
                // String id = record.topic() + "_" + record.partition() + "_" + record.offset(); // Guaranteed to be unique

                // 2. Twitter generated ID - there is an ID in the payload we get from twitter
                try { // This is because there could be a null pointer exception in that function if there is no str_id
                    String id = extractIDFromString(record.value());

                    IndexRequest indexRequest = new IndexRequest( // CONSUMER NOT SO QUICK BECAUSE WE MAKE ONE INDEXREQUEST PER RECORD - SO TO MAKE IT BETTER WE CAN USE BATCHING
                            "twitter",
                            "tweets",
                            id) // the new thing
                            .source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest); //we add to our bulk request (takes no time)
                } catch (NullPointerException e) {
                    logger.warn("Skipping bad data" + record.value());
                }
            }
            if (recordCount > 0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting the offsets");
                consumer.commitSync();
                logger.info("Offsets have been committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        //Step 3 : Close the client gracefully
        //client.close();
    }

}

