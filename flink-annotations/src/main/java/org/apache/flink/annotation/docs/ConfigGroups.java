package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * 在包含配置选项的类上使用的注释，允许根据键前缀将选项分离到不同的表中。如果选项键匹配组前缀，则配置选项被分配给{@link ConfigGroup}。当一个键匹配多个前缀时，匹配时间最长的前缀优先。选项永远不会分配给多个组。不匹配任何组的选项将隐式添加到默认组。
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Internal
public @interface ConfigGroups {
}
