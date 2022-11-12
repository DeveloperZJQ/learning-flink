package com.happy.connectors;

import com.happy.common.model.MetricEvent;
import com.happy.common.model.Rule;
import com.happy.common.utils.ExecutionEnvUtil;
import com.happy.common.utils.KafkaConfigUtil;
import com.happy.common.utils.MySQLUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author happy
 * @since 2020-07-12 07:51
 * @link <a href="https://github.com/zhisheng17/flink-learning/blob/master/flink-learning-data-sources/src/main/java/com/zhisheng/data/sources/ScheduleMain.java">...</a>
 */

@Slf4j
public class ScheduleMain {

    public static List<Rule> rules;

    public static void main(String[] args) throws Exception {
        //定时捞取规则，每隔一分钟捞一次
        ScheduledExecutorService threadPool = Executors.newScheduledThreadPool(1);
        threadPool.scheduleAtFixedRate(new GetRulesJob(), 0, 1, TimeUnit.MINUTES);

        final ParameterTool parameterTool   = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env      = ExecutionEnvUtil.prepare(parameterTool);

        DataStreamSource<MetricEvent> source = KafkaConfigUtil.buildSource(env);
        source.map((MapFunction<MetricEvent, MetricEvent>) value -> {
            if (rules.size() <= 2) {
                System.out.println("===========2");
            } else {
                System.out.println("===========3");
            }
            return value;
        }).print();

        env.execute("schedule");
    }


    static class GetRulesJob implements Runnable {
        @Override
        public void run() {
            try {
                rules = getRules();
            } catch (SQLException e) {
                log.error("get rules from mysql has an error {}", e.getMessage());
            }
        }
    }


    private static List<Rule> getRules() throws SQLException {
        System.out.println("-----get rule");
        String sql = "select * from rule";

        Connection connection   = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "123456");

        PreparedStatement ps    = connection.prepareStatement(sql);
        ResultSet resultSet     = ps.executeQuery();

        List<Rule> list         = new ArrayList<>();
        while (resultSet.next()) {
            list.add(Rule.builder()
                    .id(resultSet.getString("id"))
                    .name(resultSet.getString("name"))
                    .type(resultSet.getString("type"))
                    .measurement(resultSet.getString("measurement"))
                    .threshold(resultSet.getString("threshold"))
                    .level(resultSet.getString("level"))
                    .targetType(resultSet.getString("target_type"))
                    .targetId(resultSet.getString("target_id"))
                    .webhook(resultSet.getString("webhook"))
                    .build()
            );
        }

        return list;
    }
}