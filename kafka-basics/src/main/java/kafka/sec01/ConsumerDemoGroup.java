package kafka.sec01;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoGroup {
    public static void main(String[] args) {
        Logger logger= LoggerFactory.getLogger(ConsumerDemoGroup.class);
        String bootstrapServer="127.0.0.1:9092";
        String groupId="second_group";
        Properties properties=new Properties();
        String topic="second_topic";

        //create cosumer config
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");

        //create consumer
        KafkaConsumer<String,String> consumer= new KafkaConsumer<String, String>(properties);

        //subscribe to producer
        consumer.subscribe(Arrays.asList(topic));

        //poll for new data;
        while (true){
         ConsumerRecords<String,String> record= consumer.poll(Duration.ofMillis(1000));

         for (ConsumerRecord record1:record){
             System.out.println("Key: "+record1.key()+"\tVal: "+record1.value());
             System.out.println("Partition: "+record1.partition()+"\tOffset: "+record1.offset());
         }
        }

    }

}
