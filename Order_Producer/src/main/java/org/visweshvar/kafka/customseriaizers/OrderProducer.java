package org.visweshvar.kafka.customseriaizers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.visweshvar.kafka.OrderCallBack;

import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // ctrl N in IntelliJ / ctrl shift t in eclipse
        props.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer","org.visweshvar.kafka.customseriaizers.OrderSerializer");

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        ProducerRecord<String, Order> record = new ProducerRecord<>("OrderCSTopic", "Visweshvar" ,new Order("Visweshvar","ASUS A15",5));
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
