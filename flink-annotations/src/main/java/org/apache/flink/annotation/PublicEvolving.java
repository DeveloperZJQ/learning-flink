package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 标记类和方法供公众使用的注释，但接口在不断变化。
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
@Public
public @interface PublicEvolving {
}
