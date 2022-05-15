package com.atguigu.gmall.realtime.utils;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/8 8:41 PM
 */
public class PhoenixUtil {
    private static Connection conn = null;

    public static void init(){
        try{
            //注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //获取Phoenix的连接
            conn = DriverManager.getConnection(GmallConfig.PHOENIX_SERVER);
            //指定操作的表空间
            conn.setSchema(GmallConfig.HBASE_SCHEMA);
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    // 从Phoenix中查询数据
    // select * from 表 where XXX=xxx
    //返回类型为class<T>；
    public static <T> List<T> queryList(String sql,Class<T>clazz){
        if(conn == null){
            init();
        }
        //查询到内容后一条一条的返回到List中；
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;
        try{
            
        }catch(Exception e){
            e.printStackTrace();
            throw new RuntimeException("从维度表中查询数据失败");
        }finally{
            //释放资源
            if(rs!=null){
                try{
                    rs.close();
                }catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if(ps != null){
                try{
                    ps.close();
                }catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }
    
    public static void main(String[] args){
        //运行测试；
        System.out.println(queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
    }
}
