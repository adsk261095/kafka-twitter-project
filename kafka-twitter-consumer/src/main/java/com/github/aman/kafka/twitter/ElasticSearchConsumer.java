package com.github.aman.kafka.twitter;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.Credentials;
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



public class ElasticSearchConsumer {
    public static RestHighLevelClient createClient(){
        final String hostname = "abc";
        final String username = "abc";
        final String password = "abc";
        final CredentialsProvider credentialProvider = new BasicCredentialsProvider();
        credentialProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https")
        ).setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
            @Override
            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider);
            }
        });
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public static KafkaConsumer<String, String> createConsumer(String topic){
        String bootstrapServers = "localhost:9092";
        String groupId = "kafka-elastic-search";
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

        //create consumer config
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");

        //create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //subscribe to the topic
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    private static JsonParser jsonParser = new JsonParser();
    private static String extractIdFromTweet(String value){
        String id = jsonParser.parse(value).getAsJsonObject().get("id_str").getAsString();
        return id;
    }
    public static void main(String[] args) throws IOException {
        final String topicName = "twitter-tweets";
        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        RestHighLevelClient client = createClient();
        KafkaConsumer<String,String>  consumer = createConsumer(topicName);
        String id;
        //poll for new data
        while (true) {//will listen infinitely

            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(10));// new in kafka 2.0.0

            Integer recordCount = records.count();
            logger.info("no. of records received are: " + recordCount);
            BulkRequest bulkRequest = new BulkRequest();
            for (ConsumerRecord<String, String> record: records){
                //for every record in the batch we receive we insert it  into elastic search
                //here we will insert data into Elastic Search

                /**two ways to generate id is are:
                 *method 1: make generic id
                 * String id = record.topic() + "_" + record.partition() + "_" + record.offset();
                 * method 2: extracting it from the records itself
                 **/
                try{
                id = extractIdFromTweet(record.value());
//                System.out.println("the record id is:" + id);

                //need to pass id  to make our consumer idempodent

                    IndexRequest indexRequest = new IndexRequest(
                            "twitter",
                            "tweets",
                            id
                    ).source(record.value(), XContentType.JSON);

                    bulkRequest.add(indexRequest);
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info(indexResponse.getId());
//                    Thread.sleep(1000);//introduce delay
                } catch (Exception e) {
                    e.printStackTrace();
                }
//                break;
            }
            if(recordCount>0) {
                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Commiting the offsets");
                consumer.commitSync();
                logger.info("Offsets have been committed");
            }

//            break;
        }

        //close client gracefully
//        client.close();
    }
}
