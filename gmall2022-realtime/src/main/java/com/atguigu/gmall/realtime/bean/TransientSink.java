package com.atguigu.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author yuting
 * @version 1.0
 * @date 2022/5/15 9:02 AM
 */
@Target(ElementType.FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
