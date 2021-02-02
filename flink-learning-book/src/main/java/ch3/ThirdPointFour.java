package ch3;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;
import java.util.List;

/**
 * @author happy
 * @since 2021-02-02
 */
public class ThirdPointFour {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /**
         * BasicTypeInfo
         */
        String basicTypeInfo = "Flink通过实现BasicTypeInfo数据类型,能够支持任意Java原生基本类型(装箱)或String类型,例如Integer、String、Double等" +
                "如下代码所示,通过从给定的元素集中创建DataStream数据集";
        //创建Integer类型的数据集
        DataStreamSource<Integer> integerDataStreamSource = env.fromElements(3, 1, 2, 1, 5);
        //创建String类型的数据集
        DataStreamSource<String> stringDataStreamSource = env.fromElements("hello", "flink");

        /**
         * BasicArrayTypeInfo 对应的是Java基本类型数据(装箱)或String对象的数据,下面代码示例
         */
        DataStreamSource<List<Integer>> listDataStreamSource = env.fromElements(Arrays.asList(3, 1, 2, 1, 5));

        /**
         * TupleTypeInfo 来描述Tuple类型的数据,Flink在java接口中定义了元组类(Tuple)供用户使用. Flink Tuples是固定长度固定类型的java Tuple实现，不支持
         * 空值存储,目前支持任意的Flink java Tuple类型字段数量上限为25,如果字段数量超过上限,可以用过继承Tuple类的方式进行拓展。
         */
        DataStreamSource<Tuple2<String, Integer>> tuple2DataStreamSource = env.fromElements(new Tuple2<>("a", 1), new Tuple2<>("b", 2));

        /**
         * Scala Case Class类型 Flink通过实现CaseClassTypeInfo支持任意的Scala Case Class ,包括Scala tuples类型,支持的字段上线数量为22,支持通过字段名称和位置索引获取指标
         * 不支持存储空值. 无scala case class示例
         */

        /**
         * POJOs类型, POJOs类可以完成复杂数据结构的定义,Flink 通过实现PojoTypeInfo来描述任意的POJOs,包括Java和Scala类. 在Flink中使用POJOs类可以通过字段名称获取字段
         * 1. POJOs类必须是Public修饰且必须独立定义,不能是内部类;
         * 2. POJOs类中必须含有默认空构造器;
         * 3. POJOs类中所有的Fields必须是Public或者具有Public修饰的getter和setter方法;
         * 4. POJOs类中的字段类型必须是Flink支持的。
         */

        /**
         * Flink Value类型
         * Value数据类型实现了org.apache.flink.types.Value,其中包括read()和write()两个方法完成序列化和反序列化操作,相对于通用的序列化工具会有着比较高效的性能.
         * 目前Flink提供了内建的Value类型有IntValue、DoubleValue以及StringValue等,用户可以结合原生数据类型和Value类型使用.
         *
         */

        /**
         * 特殊数据类型-在Flink中也支持一些比较特殊的数据结构类型,例如Scala中的list、map、either、option、try数据类型,以及java中Either数据类型,还有Hadoop的Writable
         * 数据类型。
         */

        env.execute(ThirdPointFour.class.getSimpleName());
    }
}
