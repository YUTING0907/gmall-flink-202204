package com.atguigu.gmall.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.app.function.CustomerDeserialization;
import com.atguigu.gmall.realtime.app.function.DimSinkFunction;
import com.atguigu.gmall.realtime.app.function.TableProcessFunction;
import com.atguigu.gmall.realtime.bean.TableProcess;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/18 7:33 AM
 */
// 数据流：web/app -> nginx -> SpringBoot -> mysql ->flinkApp -> kafka(ods) -> FlinkApp -> kafka(dwd)/Phoenix(dim)
// 程序：mockDb -> Mysql -> FlinkCDC -> kafka(zk) -> BaseDBApp -> kafka/Phoenix(hbase,zk,hdfs)
public class BaseDBApp {
    public static void main(String[] args) throws Exception{
        //TODO:1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //TODO:2.消费kafka ods_base_db 主题数据创建流
        String sourceTopic = "ods_base_db";
        String groupId = "base_db_app_210325";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getKafkaConsumer(sourceTopic, groupId));

        //TODO:3.将每行数据转换为JSON对象并过滤(delete) 主流
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject).filter(new FilterFunction<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                //取出数据的操作类型
                String type = value.getString("type");
                return !"delete".equals(type);
            }
        });

        //TODO 4.使用FlinkCDC消费配置表并处理成广播流
         MySqlSource<String> sourceFunction = MySqlSource.<String>builder().hostname("10.39.231.163")
                .port(3306)
                .username("root")
                .password("biwN#A1A54n03$Bb")
                .databaseList("gmall2021_realtime")
                .tableList("gmall2021_realtime.table_process")
                .startupOptions(StartupOptions.initial())
                .deserializer(new CustomerDeserialization())
                .build();

        DataStreamSource<String> tableProcessStrDS = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(),"table_process");
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("map-state", String.class, TableProcess.class);
        BroadcastStream<String> broadcastStream = tableProcessStrDS.broadcast(mapStateDescriptor);

        //TODO 5.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> connectedStream = jsonObjDS.connect(broadcastStream);
        //TODO 6.分流  处理数据  广播流数据,主流数据(根据广播流数据进行处理)
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>("hbase-tag") {
        };
        SingleOutputStreamOperator<JSONObject> kafka = connectedStream.process(new TableProcessFunction(hbaseTag, mapStateDescriptor));
        //TODO 7.提取Kafka流数据和HBase流数据
        DataStream<JSONObject> hbase = kafka.getSideOutput(hbaseTag);
        //TODO 8.将Kafka数据写入Kafka主题,将HBase数据写入Phoenix表
        kafka.print("Kafka>>>>>>>");
        hbase.print("HBase>>>>>>>>>");

        hbase.addSink(new DimSinkFunction());
        kafka.addSink(MyKafkaUtil.getKafkaProducer((new KafkaSerializationSchema<JSONObject>() {
            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                return new ProducerRecord<byte[],byte[]>(element.getString("sinkTable"),element.getString("after").getBytes());
            }
        })));
        //TODO 9.启动任务
        env.execute("BaseDBApp");

    }
}
