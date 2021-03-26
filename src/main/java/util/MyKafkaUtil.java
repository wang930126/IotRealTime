package util;

import com.alibaba.fastjson.JSONObject;
import common.Constant;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class MyKafkaUtil {

    public static String DEFAULT_TOPIC = "default";

    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId) {
        Properties pro = new Properties();
        pro.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Constant.KAFKA_BOOTSTRAP_SERVERS);
        pro.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        pro.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        pro.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                pro
        );
    }

    public static FlinkKafkaProducer<String> getKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>(
                Constant.KAFKA_BOOTSTRAP_SERVERS,
                topic,
                new SimpleStringSchema()
        );
    }

    public static <T> FlinkKafkaProducer<T> getKafkaProducerBySchema(KafkaSerializationSchema<T> schema) {
        Properties pro = new Properties();
        pro.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        pro.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "" + 60 * 1000);//生产者生成数据的超时时间
        return new FlinkKafkaProducer<T>(
                DEFAULT_TOPIC,
                schema,
                pro,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                );
    }

}
