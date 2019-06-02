package com.github.aman.kafka.twitter;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;
import java.util.Properties;

public class KafkaStream {
    private static JsonParser jsonParser = new JsonParser();
    private static Integer extractUserFollowersFromTweets(String value){
        try{
            return jsonParser.parse(value)
                            .getAsJsonObject()
                            .get("user")
                            .getAsJsonObject()
                            .get("followers_count")
                            .getAsInt();
        }
        catch(NullPointerException e){
            return 0;
        }
    }

    public static void main(String[] args) {

        final String bootstrapServer = "127.0.0.1:9092";
        final String applicationId = "kafka-streams-twitter-1";
        final String topicName = "twitter-tweets";
        Logger logger = LoggerFactory.getLogger(KafkaStream.class.getName());
        //create properties
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

        //create topology
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //input topic
        KStream<String, String> inputTopic = streamsBuilder.stream(topicName);
        KStream<String, String> filteredStream = inputTopic.filter(
                //filter tweets of user which have over 1000 followers

                (k, jsonObject) -> {
                    logger.info(jsonObject);
                    return extractUserFollowersFromTweets(jsonObject) > 1000;
                }
        );

        //put the filtered tweets into topic called "important_tweets"
        filteredStream.to("important-tweets");
        //build topology
        KafkaStreams kafkaStreams = new KafkaStreams(
                streamsBuilder.build(),
                properties
        );

        //start your stream application
        kafkaStreams.start();
    }
}
