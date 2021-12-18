package com.simple.sumit.kafka;

import com.google.gson.JsonParser;
import lombok.extern.slf4j.Slf4j;
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

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Slf4j
public class ElasticSearchConsumerBulkInsert {
    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("starting main function....");
        String topic="twitter_tweets";
        RestHighLevelClient client= createClient();

        KafkaConsumer<String,String> consumer= createConsumer(topic);
        BulkRequest  bulkRequest=new BulkRequest();
        //poll for new data;
        while (true){
            ConsumerRecords<String,String> record= consumer.poll(Duration.ofMillis(100));
            log.info("Received \t "+record.count()+" record");
            int recCount=record.count();
            for (ConsumerRecord<String,String> record1:record){
                //here we insert data into elastic search.
                try{
                    String id= extrctIdFromTweet(record1.value());

                    IndexRequest indexRequest= new IndexRequest(
                            "twitter",
                            "tweets",
                            id //this is to make our consumer to  idempotent
                    ).source(record1.value(), XContentType.JSON);
                    bulkRequest.add(indexRequest);
                }catch (NullPointerException ex){
                    log.warn("skipping bad data:\t"+record1.value());
                }
            }
            if(recCount>0){
                try {
                    BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                }catch (Exception e){
                    log.error(e.getMessage());
                }
                log.info("Committing the offset");
                consumer.commitAsync();
                log.info("Offset committed");
            }

        }
       // client.close();


    }
    public static RestHighLevelClient createClient(){
        //https://w3i4gt895z:q375xgades@kafka-project-4013611778.us-east-1.bonsaisearch.net:443
        String hostname="kafka-project-4013611778.us-east-1.bonsaisearch.net";
        String username="w3i4gt895z";
        String password="q375xgades";

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username,password));
        RestClientBuilder builder= RestClient.builder(new HttpHost(hostname,443,"https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });
        RestHighLevelClient client=new RestHighLevelClient(builder);
        return client;

    }
    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServer="127.0.0.1:9092";
        String groupId="second_group";
        Properties properties=new Properties();

        //create consumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");//disable auto commit offset
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");//

        //create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<>(properties);

        //subscribe to producer
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }
    private static JsonParser jsonParser = new JsonParser();
    public static String extrctIdFromTweet(String tweet){
                try{
                    return jsonParser.parse(tweet)
                .getAsJsonObject()
                .get("id_str")
                .getAsString();
                } catch (Exception ex){
                    return tweet;
                }
    }

}
