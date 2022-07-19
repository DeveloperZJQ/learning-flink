package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 标记用作稳定的方法 ， 公共的api作为内部的开发者api
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.FIELD})
@Public
public @interface Internal {
}