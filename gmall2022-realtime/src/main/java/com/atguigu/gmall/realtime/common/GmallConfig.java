package com.atguigu.gmall.realtime.common;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/19 10:42 PM
 */
public class GmallConfig {
    //Phoenix
    public static final String HBASE_SCHEMA = "GMALL2021_REALTIME";

    //Phoenix
    public static final String PHOENIX_DRIVER = "org.apache.phoenix.jdbc.PhoenixDriver";

    public static final String PHOENIX_SERVER = "jdbc:phoenix:10.39.231.163:2181";

    //ClickHouse_Url
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop102:8123/default";

    //ClickHouse_Driver
    public static final String CLICKHOUSE_DRIVER = "ru.yandex.clickhouse.ClickHouseDriver";



}
