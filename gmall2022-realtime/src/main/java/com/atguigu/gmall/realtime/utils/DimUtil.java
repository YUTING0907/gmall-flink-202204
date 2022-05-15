package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.List;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/8 8:35 PM
 */
public class DimUtil {
    //从Phoenix中查询数据，没有使用缓存
    //封装成一个JsonObject对象；
    //hbase中的数据类型都是String；
    //Tuple2<String, String>...；...表示可变长参数；
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue ){
        //拼接查询条件
        String whereSql=" where ";
        for(int i=0;i<cloNameAndValue.length;i++){
            Tuple2<String,String> tuple2 = cloNameAndValue[i];
            String filedName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if(i>0){
                whereSql += " and ";
            }
            whereSql += filedName + "='" + fieldValue + "'";//where xx1 = 'xxx1' and xx2 = 'xxx2';xx1和xx2这里是数字，所以要用单引号；
        }

        String sql = "select * from " + tableName + whereSql;
        System.out.println("查询维度的SQL:" + sql);
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject dimJsonObj = null;
        //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
        if(dimList != null && dimList.size()>0){
            //一条数据，直接get(0)即可；
            dimJsonObj = dimList.get(0);
        }else{
            System.out.println("维度数据没有找到:" + sql);
            System.out.println();
        }
        return dimJsonObj;
    }

    //在做维度关联的时候，大部分场景都是通过id进行关联，所以提供一个方法，只需要将id的值作为参数传进来即可
    public static JSONObject getDimInfo(String tableName,String id){
        return getDimInfo(tableName, Tuple2.of("id", id));
    }

    /*
        优化：从Phoenix中查询数据，加入了旁路缓存
             先从缓存查询，如果缓存没有查到数据，再到Phoenix查询，并将查询结果放到缓存中

        redis
            类型：    string
            Key:     dim:表名:值       例如：dim:DIM_BASE_TRADEMARK:10_xxx
            value：  通过PhoenixUtil到维度表中查询数据，取出第一条并将其转换为json字符串
            失效时间:  24*3600

        //"DIM_BASE_TRADEMARK", Tuple2.of("id", "13"),Tuple2.of("tm_name","zz"))

        redisKey= "dim:dim_base_trademark:"
        where id='13'  and tm_name='zz'


        dim:dim_base_trademark:13_zz ----->Json

        dim:dim_base_trademark:13_zz
    */
//具体优化代码；
    //查询redis中是否右对应的缓存数据，若无，先向hbase表中查询数据，查询到的数据缓存到redis中；
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";
        //用redis的key来查询redis数据；
        //key的名字可以自己定义；这里redis的key也做一个拼接：dim:表名:值； 值的部分就是xx_xx_xx的类型；类似：dim:dim_base_trademark:20；
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String filedName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if (i > 0) {
                whereSql += " and ";
                redisKey += "_";
            }
            whereSql += filedName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }

        //从Redis中获取数据
        Jedis jedis = null;
        //维度数据的json字符串形式
        String dimJsonStr = null;
        //维度数据的json对象形式
        JSONObject dimJsonObj = null;
        try {
            //获取jedis客户端
            jedis = RedisUtil.getJedis();
            //根据key到Redis中查询
            dimJsonStr = jedis.get(redisKey);
            //System.out.println("rediskey:" + redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从redis中查询维度失败");
        }

        //判断是否从Redis中查询到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //如果在Redis中没有查到数据，需要到Phoenix中查询
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList != null && dimList.size() > 0) {
                dimJsonObj = dimList.get(0);
                //将查询出来的数据放到Redis中缓存起来
                if (jedis != null) {
                    //包含了过期时间；
                    jedis.setex(redisKey, 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("维度数据没有找到:" + sql);
            }
        }

        //关闭Jedis
        if (jedis != null) {
            jedis.close();
        }

        return dimJsonObj;
    }

    //根据key让Redis中的缓存失效
    //在DimSink中调用；匹配到"update"指令时，删除对应的缓存；
    //通过id来进行匹配；
    public static void deleteCached(String tableName, String id) {
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }

    //测试update指令是否会删除缓存；
    //zk、kk、maxwell、hdfs、hbase、BaseDBApp、redis、提前在配置表中加上base_trademark和update的数据；
    //redis先查看是否有缓存数据；
    //提前缓存一条数据，然后去修改一条base_trademark中的name信息，注意，修改的就是redis中存在的那条对应数据的id；
    //修改完之后，redis中缓存数据应该消失了，BaseDBApp中应该出一条向Phoenix插入数据的SQL:upsert into GMALL0709_REALTIME.DIM_BASE_TRADEMARK(tm_name,id) values ('21','20')的信息；

    //测试；
    public static void main(String[] args) {
        //System.out.println(PhoenixUtil.queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
        //写了这个工具类后的查询方式：
        //JSONObject dimInfo = DimUtil.getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "20"));
        //查询结果为：
        //查询维度的SQL:select * from DIM_BASE_TRADEMARK where id='20'
        //{"ID":"20","TM_NAME":"20"}

        JSONObject dimInfo = DimUtil.getDimInfo("DIM_BASE_TRADEMARK", "20");

        System.out.println(dimInfo);
    }
}
