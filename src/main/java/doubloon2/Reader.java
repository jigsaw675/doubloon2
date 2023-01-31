package doubloon2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
public class Reader implements Consumer {
    private final KafkaConsumer<String,String> kafkaConsumer;
    private final String topic;
    public Reader(String servers, String groupId, String topic){
        this.kafkaConsumer = new KafkaConsumer<String, String>(Consumer.createConfig(servers, groupId));
        this.topic = topic;
    }
    public void run(Producer producer){
        kafkaConsumer.subscribe(java.util.Arrays.asList(this.topic));
        while(true){
            ConsumerRecords<String,String> records = kafkaConsumer.poll(100);
            for(ConsumerRecord<String,String> record:records){
                producer.produce(record.value());
            }
        }
    }
}
