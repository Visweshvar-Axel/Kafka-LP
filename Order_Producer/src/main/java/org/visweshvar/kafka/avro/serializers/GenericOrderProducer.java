package org.visweshvar.kafka.avro.serializers;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.visweshvar.kafka.avro.Order;

import java.util.Properties;

public class GenericOrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers","localhost:9092");
        // new here
        props.setProperty("key.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        props.setProperty("schema.registry.url","http://localhost:8081/");

        // new
        KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props);
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "  \"namespace\": \"org.visweshvar.kafka.avro\",\n" +
                "  \"type\": \"record\",\n" +
                "  \"name\": \"Order\",\n" +
                "  \"fields\": [\n" +
                "    {\"name\": \"customerName\", \"type\": \"string\"},\n" +
                "    {\"name\": \"product\", \"type\": \"string\"},\n" +
                "    {\"name\": \"quantity\", \"type\": \"int\"}\n" +
                "  ]\n" +
                "}");

        // new
        GenericRecord order = new GenericData.Record(schema);
        order.put("customerName","Visweshvar");
        order.put("product","ASUS A15");
        order.put("quantity",10);

        // new
        ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("OrderAvroGRTopic", order.get("customerName").toString() ,order);
        try {
            System.out.println("sending...");
            producer.send(record);
            System.out.println("Message sent successfully");
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
