package com.atguigu.gmall.realtime.bean;

import lombok.Data;

import java.math.BigDecimal;
/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/4 4:48 PM
 */
// @Data注解会自动帮你配置好get(),set()方法
@Data
public class OrderInfo {
    Long id;
    Long province_id;
    String order_status;
    Long user_id;
    BigDecimal total_amount;
    BigDecimal activity_reduce_amount;
    BigDecimal coupon_reduce_amount;
    BigDecimal original_total_amount;
    BigDecimal feight_fee;
    String expire_time;
    String create_time;  //yyyy-MM-dd HH:mm:ss
    String operate_time;
    String create_date; // ֶδõ
    String create_hour;
    Long create_ts;
}
