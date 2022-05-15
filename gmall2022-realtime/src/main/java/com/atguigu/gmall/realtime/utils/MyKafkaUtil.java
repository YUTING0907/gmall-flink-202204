package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/18 7:42 AM
 */
public class MyKafkaUtil {
    //DEFAULT_TOPIC
    //brokers
    //default_topic
    //KAFKA_SERVER
    private static String DEFAULT_TOPIC = "dwd_default_topic";
    private static String brokers = "10.63.40.20:9092,10.63.40.21:9092,10.63.40.22:9092";
    private static String default_topic = "DWD_DEFAULT_TOPIC";
    private static String KAFKA_SERVER = "10.63.40.20:9092,10.63.40.21:9092,10.63.40.22:9092";
    //properties
    private static Properties properties = new Properties();
    static{
        properties.setProperty("bootstrap.servers",KAFKA_SERVER);
    }
    public static FlinkKafkaProducer<String> getKafkaProducer(String topic){
        return new FlinkKafkaProducer<String>(brokers,topic,new SimpleStringSchema());
    }

    //getKafkaProducer
    public static<T> FlinkKafkaProducer<T> getKafkaProducer(KafkaSerializationSchema<T> kafkaSerializationSchema){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        return new FlinkKafkaProducer<T>(default_topic,kafkaSerializationSchema,properties,FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }

    //getKafkaConsumer
    public static FlinkKafkaConsumer<String> getKafkaConsumer(String topic, String groupId){
        Properties properties = new Properties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);

        return new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(),properties);
    }
    //ƴKafkaԵDDL
    public static String getKafkaDDL(String topic,String groupId){
        return " 'connector' = 'kafka', " +
                "'topic' = '" + topic + "'," +
                " 'properties.bootstrap.servers' = '" + brokers + "', " +
                " 'properties.group.id' = '" + groupId + "', " +
                " 'format' = 'json', " +
                " 'scan.startup.mode' = 'latest-offset'  ";
    }

    public static FlinkKafkaConsumer<String> getKafkaSource(String topic, String groupId){
        return null;
    }

    public static <T> FlinkKafkaProducer<T> getKafkaSinkBySchema(KafkaSerializationSchema<T> kafkaSerializationSchema){
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,5 * 60 * 1000+"");
        return new FlinkKafkaProducer<T>(DEFAULT_TOPIC, kafkaSerializationSchema, properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
    }





}
