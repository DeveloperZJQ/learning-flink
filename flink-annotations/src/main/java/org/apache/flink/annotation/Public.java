package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 被标记的类，意味着是一个公共的稳定的接口
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Documented
@Target(ElementType.TYPE)
@Public
public @interface Public {
}
