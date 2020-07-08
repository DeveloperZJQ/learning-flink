package com.stream.samples;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.LocalEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.Serializable;

/**
 * @author happy
 * @create 2020-07-08 12:54
 *  本段代码请细细品味，不能执行
 *  本段代码是记录了flink基本的操作，大概包含了，初始化flink执行环境-读取数据源的方式-执行转换操作-分区Key指定-输出结果-程序触发
 */
public class FlinkBasicOperator {
    public static void main(String[] args) throws Exception {

        /**
         * 1.初始化FLink程序执行环境  ExecutionEnvironment
         */
        //流-初始化flink执行环境--如果是在本地创建本地环境，如果是在集群上启动，则创建集群环境
        StreamExecutionEnvironment env          = StreamExecutionEnvironment.getExecutionEnvironment();
        //流-显而易见，创建本地环境，并设置并行度为5
        LocalStreamEnvironment localEnv         = StreamExecutionEnvironment.createLocalEnvironment(5);
        //流-创建远程环境，指定远程managerHost和端口，并行度及运行程序所在jar包及其依赖包，本地的程序相当于客户端
        StreamExecutionEnvironment remoteEnv    = StreamExecutionEnvironment.createRemoteEnvironment("JobManagerHost", 6021, 5, "/uesr/application.jar");

        //批-设定Flink运行环境，如果在本地启动则创建本地环境，如果在集群上启动，则创建集群环境
        ExecutionEnvironment batchEnv       = ExecutionEnvironment.getExecutionEnvironment();
        //批-指定并行度创建本地执行环境
        LocalEnvironment batchLocalEnv      = ExecutionEnvironment.createLocalEnvironment(5);
        //批-指定远程JobManagerIp和RPC端口及运行程序所在jar包及其依赖包
        ExecutionEnvironment remoteBatchEnv = ExecutionEnvironment.createRemoteEnvironment("JobManagerIp", 6021, 5, "/user/application.jar");

        /**
         * 2.初始化数据
         * 数据源接入这块，FLink自己封装的api特别丰富，下面只是举一个简单的示例
         */

        //读取本地文件
        DataStreamSource<String> stringDataStreamSource = env.readTextFile("file://path/file"); //流
        DataSource<String> stringDataBatchSource        = batchEnv.readTextFile("file://path/file");   //批


        /**
         * 3.执行转换操作
         * 转换操作大致分为下面几类
         * 3-1
         * 3-2
         * 3-3
         */

        //3-2 通过创建匿名类实现Function接口
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split("\\W+");

                for (String str : splits) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }
        })
                .keyBy(0)
                .sum(1);


        //3-3 通过实现RichFunction接口,通过与上面的flatMapFunction比较，相比一定看出来一些端倪，没错的，rich函数相比较function函数而言，增加了对flink状态的读写
        FlatMapOperator<String, Tuple2<String, Integer>> stringRichFlatMapOperator = stringDataBatchSource.flatMap(new RichFlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] splits = s.split("\\W+");

                for (String str : splits) {
                    collector.collect(new Tuple2<>(str, 1));
                }
            }

            @Override
            public void setRuntimeContext(RuntimeContext t) {
                super.setRuntimeContext(t);
            }

            @Override
            public RuntimeContext getRuntimeContext() {
                return super.getRuntimeContext();
            }

            @Override
            public IterationRuntimeContext getIterationRuntimeContext() {
                return super.getIterationRuntimeContext();
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }

            @Override
            public void close() throws Exception {
                super.close();
            }
        });


        /**
         * 4.分区key指定
         */

        //4-1.根据字段位置指定，在DataStream API中通过keyBy()方法将DataStream数据集根据指定的key转换成重新分区的keyedStream
        //如以下代码所示，对数据集按照相同key进行sum() 聚合操作。
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum1 = sum.keyBy(0).sum(1);  //本段代码摘自上面的flatMapFunction
        UnsortedGrouping<String> stringUnsortedGrouping = stringDataBatchSource.groupBy(0);//DataSet作用于groupBy
        AggregateOperator<String> max = stringUnsortedGrouping.max(1);

        //4-2.根据字段名称指定
        //使用字段名称需要DataStream中的数据结构类型必须是Tuple类或者POJOs类的。如以下代码所示，通过指定name字段名称来确定groupby的key字段


        //4-3.通过key选择器指定,最后获取KeyedStream
        DataStreamSource<Person> personDataStreamSource = env.fromElements(new Person("hello", 12), new Person("flink", 123));
        KeyedStream<Person, String> personStringKeyedStream = personDataStreamSource.keyBy(new KeySelector<Person, String>() {
            @Override
            public String getKey(Person person) throws Exception {
                return person.getName();
            }
        });

        //5.输出结果
        //数据集经过转换操作之后，形成最终的结果数据集，一般需要将数据集输出在外部系统中或者输出在控制台之上。在FLink DataStream和DataSet接口中定义了基本的数据输出方法，例如基于文件输出WriteAsText()
        //，基于控制台输出print()等。同时Flink在系统中定义了大量的Connector，方便用户和外部系统交互，用户可以直接通过调用addSink() 添加输出系统定义的DataSink类算子，
        //这样就能将数据输出到外部系统。以下实例调用DataStream API 中的writeAsText()和print()方法将数据集输出在文件和客户端中。
        sum.writeAsText("file://path/to/savefile");//结果以txt的格式输出到本地文件
        sum.writeAsCsv("file://path/to/savefile");//结果以csv的格式输出到本地文件
        sum.print();//结果输出到控制台


        //6.程序触发
        //调用StreamExecutionEnvironment的execute方法执行流式应用程序
        env.execute("APP name");
    }
}

//3-1.通过创建Class实现Function接口
class MyMapFunction implements MapFunction<String, String> {
    @Override
    public String map(String s) throws Exception {
        return s.toLowerCase();
    }
}

class Person implements Serializable {
    private String name;
    private int age;

    public Person() {
    }

    public Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}