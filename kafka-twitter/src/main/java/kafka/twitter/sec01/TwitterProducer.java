package kafka.twitter.sec01;

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
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    TwitterProducer(){};
    String consumerKey="AaTI67VwcrDpjzzucXrRnkt1W";
    String consumerSecret="RQw5md5a88aWqA4Mb1OEJrSLz9e0coARtd4e9r6gzGd75JtdBn";
    String accessToken="1325804378356244481-gyRVluI73ea7prfSkPkYwMjpRQ7Kmo";
    String accessTokenSecret="rHQTdwZtwwmHqKzaPXXiMnI8nQNAikHqCPgKMA9ZBjpMt";
    Logger logger= LoggerFactory.getLogger(TwitterProducer.class);
    List<String> terms = Lists.newArrayList("omicrone","srk","lockdown");

    public static void main(String[] args) {
        System.out.println("Starting...");
        new TwitterProducer().run();

    }
    public void run(){
        //create kafka client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(1000);
        // Attempts to establish a connection.
        Client hosebirdClient =createClient(msgQueue);
        hosebirdClient.connect();
        //create a kafka producer
       KafkaProducer<String,String> producer= getKafkaProducer();
        String topic="twitter_tweets";
        String key=null;

        //loop to send tweets to kafka
        // on a different thread, or multiple different threads....
        while (!hosebirdClient.isDone()) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                hosebirdClient.stop();
            }catch (Exception e){
                System.out.println("Exception occurred+"+e.getMessage());
                hosebirdClient.stop();
            }
            if(msg != null){
                System.out.println("Received message: "+msg);
                ProducerRecord<String,String> record=
                        new ProducerRecord<>(topic, key, msg);
                producer.send(record,(recordMetadata, e) -> {
                    if(e!=null){
                        System.out.println("Something went wrong while publishing message");
                    }
                });
                producer.flush();
            }
        }
        //closing application
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            System.out.println("stopping application");
            System.out.println("shutting down from twitter client");
            hosebirdClient.stop();
            System.out.println("Shutting down producer");
            producer.close();
            System.out.println("Done");

        }));
        System.out.println("End of application");

    }
    public Client createClient(BlockingQueue<String> msgQueue){

        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        hosebirdEndpoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1( consumerKey, consumerSecret, accessToken, accessTokenSecret);

        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))

                ;
        Client hosebirdClient = builder.build();
        return hosebirdClient;
    }
    public KafkaProducer<String,String> getKafkaProducer(){
        Properties properties=new Properties();
        String bootstrapServer="127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        return new KafkaProducer<String, String>(properties);
    }
}
