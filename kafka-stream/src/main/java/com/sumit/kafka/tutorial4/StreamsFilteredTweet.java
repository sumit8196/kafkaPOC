package com.sumit.kafka.tutorial4;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
@Slf4j
public class StreamsFilteredTweet {
    public static void main(String[] args) {
        //create property
        Properties properties =new Properties();
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,"kafka-stream-demo");
        properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        //create topology
        StreamsBuilder streamsBuilder =new StreamsBuilder();
        KStream<String,String> inputTopic = streamsBuilder.stream("twitter_tweets");
        KStream<String,String> filteredStream = inputTopic.filter((key, jsonTweet) -> extractFollowerCountTweet(jsonTweet) > 10000);
        filteredStream.to("important_tweets");
        log.info("filtered topics are\t"+filteredStream.toString());
        //build topology
        KafkaStreams kafkaStreams= new KafkaStreams(streamsBuilder.build(),properties);
        kafkaStreams.start();


    }
    private static JsonParser jsonParser = new JsonParser();
    public static Integer extractFollowerCountTweet(String tweet){
        try{
            return jsonParser.parse(tweet)
                    .getAsJsonObject()
                    .get("user")
                    .getAsJsonObject()
                    .get("follower_count")
                    .getAsInt();
        }catch (Exception ex){
            return 0;
        }
    }

}
