package org.visweshvar.kafka.avro.deserializers;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.visweshvar.kafka.avro.Order;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class GenericOrderConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // new here
        props.setProperty("key.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        props.setProperty("schema.registry.url","http://localhost:8081/");
        // new we dont need specific when we use generic record
//        props.setProperty("specific.avro.reader","true");
        props.setProperty("group.id","OrderGroup");

        // change into generic new
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("OrderAvroGRTopic"));

        ConsumerRecords<String, GenericRecord> records = consumer.poll(Duration.ofSeconds(60));
        for (ConsumerRecord<String, GenericRecord> record : records) {
            String customerName = record.key();
            // jackson requires empty constructor to create object
            GenericRecord order = record.value();
            if(order != null) {
                System.out.println("Customer Name: " + customerName);
                // need to use get method
                System.out.println("Product: " + order.get("product"));
                System.out.println("Quantity: " + order.get("quantity"));
            } else System.out.println("order null");
        }
        consumer.close();
    }
}
