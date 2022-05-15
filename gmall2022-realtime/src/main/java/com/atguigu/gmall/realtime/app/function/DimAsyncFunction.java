package com.atguigu.gmall.realtime.app.function;

import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.DimUtil;
import com.atguigu.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.DriverManager;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/8 7:54 PM
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    //线程池对象的父接口生命（多态）
    private ExecutorService executorService;
    //构造器，这样就可以自定义具体的维度表了；
    //维度的表名
    private String tableName;
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }
    //这两个方法放到了接口方法中；这样就把方法、变量和构造器分开来写；
    //public abstract void getKey (OrderWide orderWide);
    //public abstract void join(T obj,JSONObject dimInfoJsonObj);
    @Override
    public void open(Configuration parameters) throws Exception {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //如果需要处理超时数据，还可以重写timeOut方法；
        executorService.submit(
                new Runnable() {
                    @Override
                    public void run() {
                        try {
                            //发送异步请求
                            long start = System.currentTimeMillis();
                            //从流中事实数据获取key，在本项目中，key是order_id;
                            //问题在于如何获取这个key；
                            //这里是无法获取的，那么我们就把他定义成一个抽象的方法，把具体的方法实现放到能获取到key的位置去重写实现；
                            String key = getKey(obj);

                            //根据维度的主键到维度表中进行查询
                            JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                            //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                            if(dimInfoJsonObj != null){
                                //维度关联  流中的事实数据和查询出来的维度数据进行关联
                                //这里同样采用抽象方法来做；
                                //TODO  需要实际实现的方法，从维表中获取维度数据，补充到宽表数据中   维度数据和流数据关联，
                                join(obj,dimInfoJsonObj);
                            }
                            //System.out.println("维度关联后的对象:" + obj);
                            long end = System.currentTimeMillis();
                            System.out.println("异步维度查询耗时" +(end -start)+"毫秒");
                            //将关联后的数据数据继续向下传递
                            resultFuture.complete(Arrays.asList(obj));
                        } catch (Exception e) {
                            e.printStackTrace();
                            throw new RuntimeException(tableName + "维度异步查询失败");
                        }
                    }
                }
        );

    }
    //超时处理
    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {

    }

}
