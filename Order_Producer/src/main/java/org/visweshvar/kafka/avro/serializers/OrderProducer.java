package org.visweshvar.kafka.avro.serializers;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.visweshvar.kafka.avro.Order;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // new here
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url","http://localhost:8081/");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        Order order = new Order("Visweshvar","ASUS A15",5);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderAvroTopic", order.getCustomerName().toString() ,order);
        try {
            producer.send(record);
            System.out.println("Message sent successfully");
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
