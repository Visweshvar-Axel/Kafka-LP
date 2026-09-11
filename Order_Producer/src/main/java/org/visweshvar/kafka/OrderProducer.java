package org.visweshvar.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // ctrl N in IntelliJ / ctrl shift t in eclipse
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.apache.kafka.common.serialization.IntegerSerializer");

        KafkaProducer<String, Integer> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Integer> record = new ProducerRecord<>("OrderTopic", "TUF A14 gaming laptop" ,10);
        try {
            // Synchronous call
            RecordMetadata recordMetadata = producer.send(record).get();
            System.out.println(recordMetadata.partition());
            System.out.println(recordMetadata.offset());
//            // alt enter for action
//            // future metadata
//            Future<RecordMetadata> send = producer.send(record);
//            // fire and forget
//            producer.send(record);
            System.out.println("Message sent successfully");
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
