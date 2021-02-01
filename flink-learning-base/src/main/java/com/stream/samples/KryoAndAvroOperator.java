package com.stream.samples;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author happy
 * @since 2020-12-10
 */
public class KryoAndAvroOperator {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment enc = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = enc.socketTextStream("127.0.0.1", 9001);

        enc.getConfig().enableForceAvro();//开启Avro
        enc.getConfig().enableForceKryo();//开启Kryo
//        enc.getConfig().addDefaultKryoSerializer();   //如果kryo不能序列化POJOS对象，那么可以自定义kryo
        enc.execute(KryoAndAvroOperator.class.getSimpleName());
    }
}
