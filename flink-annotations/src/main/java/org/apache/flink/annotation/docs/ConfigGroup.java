package org.apache.flink.annotation.docs;

import org.apache.flink.annotation.Internal;

import java.lang.annotation.Target;

/**
 * 指定一组配置选项的类。组名将用作生成的html文件文件名的基础，定义在{@link ConfigOptionsDocGenerator}中。
 *
 * @see ConfigGroups
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Target({})
@Internal
public @interface ConfigGroup {
}
