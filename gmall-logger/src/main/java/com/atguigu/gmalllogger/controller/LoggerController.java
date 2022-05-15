package com.atguigu.gmalllogger.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;


/**
 * @author yuting
 * @version 1.0
 * @date 2022/4/5 5:53 PM
 */
@RestController //@Controller + @ResponseBody
@Slf4j
public class LoggerController {

    private KafkaTemplate<String,String> kafkaTemplate;

    @RequestMapping("applog")
    public String getLogger(@RequestParam("param") String jsonStr){
        //落盘
        log.info(jsonStr);
        //写入kafka
        kafkaTemplate.send("ods_base_log",jsonStr);

        return "success";
    }

}
