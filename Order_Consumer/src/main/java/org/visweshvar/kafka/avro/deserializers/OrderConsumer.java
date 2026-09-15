package org.visweshvar.kafka.avro.deserializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.visweshvar.kafka.avro.Order;
import org.visweshvar.kafka.customseriaizers.OrderDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class OrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // new here
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url","http://localhost:8081/");
        // new
        props.setProperty("specific.avro.reader","true");
        props.setProperty("group.id","OrderGroup");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroTopic"));

        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofSeconds(20));
        for (ConsumerRecord<String, Order> record : records) {
            String customerName = record.key();
            // jackson requires empty constructor to create object
            Order order = record.value();
            if(order != null) {
                System.out.println("Customer Name: " + customerName);
                System.out.println("Product: " + order.getProduct());
                System.out.println("Quantity: " + order.getQuantity());
            } else System.out.println("order null");
        }
        consumer.close();
    }
}
