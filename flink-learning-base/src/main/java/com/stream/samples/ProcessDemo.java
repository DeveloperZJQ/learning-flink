package com.stream.samples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author happy
 * @create 2020-07-11 12:10
 */
public class ProcessDemo {
    public static void main(String[] args) throws Exception {
        if (args.length!=2){
            System.err.println("请确认输入的参数是否正确");
            return;
        }
        StreamExecutionEnvironment env                  = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource       = env.socketTextStream(args[0], Integer.parseInt(args[1]));

        //flink自带的process函数
        processFunction(env,dataStreamSource);

        //flink自定义的process函数
        userDefineProcessFunction(env,dataStreamSource);

        env.execute("ProcessDemo App start");
    }

    /**
     * flink自带的process方法
     * @param env
     * @param dataStreamSource
     */
    public static void processFunction(StreamExecutionEnvironment env,DataStreamSource<String> dataStreamSource){
        SingleOutputStreamOperator<Customer> process    = dataStreamSource.process(new ProcessFunction<String, Customer>() {
            @Override
            public void processElement(String s, Context context, Collector<Customer> collector) throws Exception {
                String[] split = s.split(",");
                Long timeStamp = Long.parseLong(split[0]);
                String userId = split[1];
                String cardId = split[2];
                String transfer = split[3];
                double fee = Double.parseDouble(split[4]);
                collector.collect(new Customer(timeStamp, userId, cardId, transfer, fee));
            }
        });
        process.print();
    }

    /**
     * 上面是flink自带的process函数，那么对应的就会有用户自定义的process函数
     * @param env
     * @param dataStreamSource
     */
    public static void userDefineProcessFunction(StreamExecutionEnvironment env,DataStreamSource<String> dataStreamSource){
        ProcessFunction<String, Customer> processFunction = new ProcessFunction<>() {
            private static final long serialVersionUID  =   1L;
            @Override
            public void processElement(String s, Context context, Collector<Customer> collector) throws Exception {
                String[] split = s.split(",");
                collector.collect(new Customer(Long.parseLong(split[0]),split[1],split[2],split[3], Double.parseDouble(split[4])));
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Customer> out) throws Exception {
                super.onTimer(timestamp, ctx, out);
            }
        };

        SingleOutputStreamOperator<Customer> processRes = dataStreamSource.process(processFunction);
        processRes.print();
    }
}


class Customer{
    public Long timeStamp;
    public String userId;
    public String cardId;
    public String transfer;
    public double fee;

    public Customer() {
    }

    public Customer(Long timeStamp, String userId, String cardId, String transfer, double fee) {
        this.timeStamp = timeStamp;
        this.userId = userId;
        this.cardId = cardId;
        this.transfer = transfer;
        this.fee = fee;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getCardId() {
        return cardId;
    }

    public void setCardId(String cardId) {
        this.cardId = cardId;
    }

    public String getTransfer() {
        return transfer;
    }

    public void setTransfer(String transfer) {
        this.transfer = transfer;
    }

    public double getFee() {
        return fee;
    }

    public void setFee(double fee) {
        this.fee = fee;
    }
}