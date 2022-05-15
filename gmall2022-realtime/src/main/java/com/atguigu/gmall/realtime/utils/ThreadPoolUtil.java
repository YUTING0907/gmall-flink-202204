package com.atguigu.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/8 8:00 PM
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    public static ThreadPoolExecutor getInstance(){
        //双重校验；
        if(pool == null){
            //开发时一般不把线程锁加到方法上；
            synchronized (ThreadPoolUtil.class){
                //这么写是为了保证单例模式；
                if(pool == null){
                    pool = new ThreadPoolExecutor(
                            4,20,300, TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
