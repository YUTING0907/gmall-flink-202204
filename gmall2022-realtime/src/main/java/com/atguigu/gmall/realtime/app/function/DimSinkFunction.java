package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/19 11:48 PM
 */
public class DimSinkFunction extends RichSinkFunction<JSONObject> {
    private Connection connection;
    //data:{"tm_name":"kevenhe","id":12}
    //SQL：upsert into db.tn(id,tm_name,aa,bb) values('...','...','...','...')
    private String genUpsertSql(String sinkTable,JSONObject data){
         Set<String> keySet = data.keySet();
         Collection<Object> values = data.values();

         //keySet.mkString(",");  =>  "id,tm_name"
        return "upsert into" + GmallConfig.HBASE_SCHEMA + "." + sinkTable + "("+
                StringUtils.join(keySet,",") + ") values('" +
                StringUtils.join(values,"',") + "')'";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName(GmallConfig.PHOENIX_DRIVER);
        connection = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
        connection.setAutoCommit(true);
    }

    @Override
    public void invoke(JSONObject value, Context context) throws Exception {
        PreparedStatement preparedStatement = null;
        try{
            //获取SQL语句
            String sinkTable = value.getString("sinkTable");
            JSONObject after = value.getJSONObject("after");
            String upsertSql = genUpsertSql(sinkTable,
                    after);
            System.out.println(upsertSql);

            //预编译SQL
            preparedStatement = connection.prepareStatement(upsertSql);

            //执行插入操作
            preparedStatement.executeUpdate();

        }catch(Exception e){
            e.printStackTrace();
        }finally{
            if(preparedStatement!=null){
                preparedStatement.close();
            }
        }
    }
}
