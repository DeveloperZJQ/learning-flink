> 参考 https://developer.aliyun.com/article/778485

# 简介
在数据库中的静态表上做 OLAP 分析时，两表 join 是非常常见的操作。同理，在流式处理作业中，有时也需要在两条流上做 join 以获得更丰富的信息。Flink DataStream API 为用户提供了3个算子来实现双流 join，分别是：1、join()；2、coGroup()；3、intervalJoin()

- join()
- coGroup()
- intervalJoin


从 Socket 分别接入点击流和订单流，并转化为 POJO。


## join
join() 算子提供的语义为"Window join"，即按照指定字段和（滚动/滑动/会话）窗口进行 inner join，支持处理时间和事件时间两种时间特征。以下示例以10秒滚动窗口，将两个流通过商品 ID 关联，取得订单流中的售价相关字段。


## coGroup()
只有 inner join 肯定还不够，如何实现 left/right outer join 呢？答案就是利用 coGroup() 算子。它的调用方式类似于 join() 算子，也需要开窗，但是 CoGroupFunction 比 JoinFunction 更加灵活，可以按照用户指定的逻辑匹配左流和/或右流的数据并输出。

以下的例子就实现了点击流 left join 订单流的功能，是很朴素的 nested loop join 思想（二重循环）。

## interval()
join() 和 coGroup() 都是基于窗口做关联的。但是在某些情况下，两条流的数据步调未必一致。例如，订单流的数据有可能在点击流的购买动作发生之后很久才被写入，如果用窗口来圈定，很容易 join 不上。所以 Flink 又提供了"Interval join"的语义，按照指定字段以及右流相对左流偏移的时间区间进行关联，即：
right.timestamp ∈ [left.timestamp + lowerBound; left.timestamp + upperBound]

interval join 也是 inner join，虽然不需要开窗，但是需要用户指定偏移区间的上下界，并且只支持事件时间。
示例代码如下。注意在运行之前，需要分别在两个流上应用 assignTimestampsAndWatermarks() 方法获取事件时间戳和水印。

由上可见，interval join 与 window join 不同，是两个 KeyedStream 之上的操作，并且需要调用 between() 方法指定偏移区间的上下界。如果想令上下界是开区间，可以调用 upperBoundExclusive()/lowerBoundExclusive() 方法。

interval join 的实现原理
以下是 KeyedStream.process(ProcessJoinFunction) 方法调用的重载方法的逻辑。