package com.atguigu.gmall.realtime.app.function;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/15 1:00 PM
 */
public class SplitFunction extends TableFunction<Row> {
    public void eval(String str){
        try {
            //
            List<String> words = KeywordUtil.splitKeyWord(str);
            for(String word :words){
                collect(Row.of(word));
            }
        }catch(IOException e){
            collect(Row.of(str));
        }
    }
}
