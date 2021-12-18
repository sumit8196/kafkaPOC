package kafka.sec01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    public static void main(String[] args) {
        final  Logger log= LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        String bootstrapServer="127.0.0.1:9092";

        //create producer property
        Properties properties=new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> producer=new KafkaProducer<String, String>(properties);

        //create a producer record
        String topic="second_topic";
        String key="id_1";
        ProducerRecord<String,String> record=
                new ProducerRecord<>(topic, key, "Hello kafka with key 1: message 17");

        //send data- asynchronous
        producer.send(record,(recordMetadata, e) -> {
            //will be called when data is successfully send or exception is thrown
            if(e == null){
                log.info("received metadata\n"+"topic:\t"+recordMetadata.topic()+"\nPartition\t"+recordMetadata.partition()
                +"\nOffset\t"+recordMetadata.offset());
                System.out.println("data is sent successfully :"+"received metadata\n"+"topic:\t"+recordMetadata.topic()+"\nPartition\t"+recordMetadata.partition()
                        +"\nOffset\t"+recordMetadata.offset());
            }else {
                log.error("Exception occurred OMG: ",e);
                System.out.println("Exeption occurred");
            }

        });
        //flush the data
        producer.flush();
        //flush the data and close the producer
        producer.close();
        System.out.println("completed");

    }
}
