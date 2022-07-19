package org.apache.flink.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Target;

/**
 * 这个注释声明一个函数、字段、构造函数或整个类型只在测试时可见。
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Documented
@Target({ElementType.TYPE, ElementType.METHOD, ElementType.FIELD, ElementType.CONSTRUCTOR})
@Internal
public @interface VisibleForTesting {
}
