package org.visweshvar.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public class OrderCallBack implements org.apache.kafka.clients.producer.Callback {
    @Override
    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
        System.out.println(recordMetadata.partition());
        System.out.println(recordMetadata.offset());
        System.out.println(recordMetadata.topic());
        System.out.println(recordMetadata.toString());
        if (e != null) e.printStackTrace();
    }
}
