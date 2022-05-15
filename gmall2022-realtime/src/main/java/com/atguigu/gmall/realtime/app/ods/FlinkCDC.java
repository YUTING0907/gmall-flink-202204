package com.atguigu.gmall.realtime.app.ods;

import com.atguigu.gmall.realtime.app.function.CustomerDeserialization;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/5 9:34 PM
 */
public class FlinkCDC {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //2.Flink-CDC 将读取 binlog 的位置信息以状态的方式保存在 CK,如果想要做到断点续传,需要从Checkpoint或者Savepoint 启动程序
        //2.1 开启 Checkpoint,每隔 5 秒钟做一次 CK
        env.enableCheckpointing(5000L);
        //2.2 指定 CK 的一致性语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //2.3 设置任务关闭的时候保留最后一次 CK 数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 指定从 CK 自动重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000L));
        //2.5 设置状态后端
        env.setStateBackend(new FsStateBackend("hdfs://10.66.231.68:8020/flinkCDC"));
        //2.6 设置访问 HDFS 的用户名
        System.setProperty("root", "Panda108");
        //3.创建 Flink-MySQL-CDC 的 Source
        MySqlSource<String> sourceFunction =MySqlSource.<String>builder()
                .hostname("10.39.231.163")
                .port(3306)
                .username("root")
                .password("biwN#A1A54n03$Bb")
                .databaseList("gmallDB")
                //.tableList("gmall-flink.z_user_info") //可选配置项,如果不指定该参数,则会
                .deserializer(new CustomerDeserialization())
                .startupOptions(StartupOptions.latest())
                .build();
        //4.使用 CDC Source 从 MySQL 读取数据

        DataStreamSource<String> mysqlDS = env.fromSource(sourceFunction, WatermarkStrategy.noWatermarks(),"flinkcdc");
        //5.打印数据
        mysqlDS.print();
        //6.执行任务
        try {
            env.execute("FlinkCDC");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
