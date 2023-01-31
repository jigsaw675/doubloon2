package doubloon2;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
public interface Producer {
    public static void write(KafkaProducer<String,String> producer, String topic, String message){
        ProducerRecord<String,String> record = new ProducerRecord<>(topic, message);
        producer.send(record);
    }
    public static Properties createConfig(String servers){
        Properties props = new Properties();
        props.put("bootstrap.servers",servers);
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",1000);
        props.put("linger.ms",1);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        return props;
    }
    public void produce(String message);
}
