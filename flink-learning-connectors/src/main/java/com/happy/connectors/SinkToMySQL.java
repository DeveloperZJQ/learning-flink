package com.happy.connectors;

import com.happy.common.model.TransInfo;
import com.happy.common.utils.ExecutionEnvUtil;
import com.happy.common.utils.GsonUtil;
import com.happy.common.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import static com.happy.common.constant.PropertiesConstants.METRICS_TOPIC;

/**
 * @author happy
 * @create 2020-07-12 08:51
 */
public class SinkToMySQL {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<TransInfo> transInfo = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, TransInfo.class)); //博客里面用的是 fastjson，这里用的是gson解析，解析字符串成 transinfo 对象

        transInfo.addSink(new SinkToMySQL2()); //数据 sink 到 mysql

        env.execute("SinkToMySQL App start");
    }
}

@Slf4j
class SinkToMySQL2 extends RichSinkFunction<TransInfo> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = getConnection();
        String sql = "insert into Student(id, name, password, age) values(?, ?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每条数据的插入都要调用一次 invoke() 方法
     *
     * @param value
     * @param context
     * @throws Exception
     */
    @Override
    public void invoke(TransInfo value, Context context) throws Exception {
        if (ps == null) {
            return;
        }
        //组装数据，执行插入操作
        ps.setLong(1, value.getTimeStamp());
        ps.setString(2, value.getCardId());
        ps.setString(3, value.getUserId());
        ps.setString(4, value.getTransfer());
        ps.setString(5,value.getFee());
        ps.executeUpdate();
    }

    private static Connection getConnection() {
        Connection con = null;
        try {
            Class.forName("com.mysql.jdbc.Driver");
            //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
            con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "123456");
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}