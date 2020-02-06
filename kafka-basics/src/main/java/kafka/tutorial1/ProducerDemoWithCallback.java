package kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String, String> record = new ProducerRecord<>("first_topoc", "test IDEA 1");
        //send data - asynchronous
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                //executes every time a record is successfully sent or an exception is thrown
                if (exception == null) {
                    //the record was successfully sent
                    logger.info("\nReceived new metadata: \n" +
                            "\tTopic: " + metadata.topic() + "\n" +
                            "\tPartition: " + metadata.partition() + "\n" +
                            "\tOffset: " + metadata.offset() + "\n" +
                            "\tTimestamp: " + metadata.timestamp());
                } else {
                    logger.error("Error while producing: ", exception);
                }
            }
        });

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
