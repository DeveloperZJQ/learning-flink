package com.happy.connectors.gbase;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Properties;

/**
 * gbase驱动包在maven中央仓库没找到，代码架子基本就这些，如果有需要的可以私密留言
 *
 * @author DeveloperZJQ
 * @since 2022-6-20
 */
public class GBaseSink extends RichSinkFunction<MessageEvent> {

    private Properties properties;
    BasicDataSource basicDataSource;
    Connection connection;
    PreparedStatement statement;

    public GBaseSink(Properties properties) {
        this.properties = properties;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化配置，程序初始化一次
        basicDataSource = new BasicDataSource();
        basicDataSource.setDriverClassName("com.gbase.jdbc.Driver");
        basicDataSource.setUrl("gbase.jdbc.url");
        basicDataSource.setUsername("your username");
        basicDataSource.setPassword("your password");
        basicDataSource.setMaxActive(3);
        basicDataSource.setMaxIdle(10);
        basicDataSource.setMaxWait(3000);
        basicDataSource.setTimeBetweenEvictionRunsMillis(60000);
        basicDataSource.setRemoveAbandoned(true);
        basicDataSource.setRemoveAbandonedTimeout(180);
        basicDataSource.setValidationQuery("select 1 from dual");
        basicDataSource.setLogAbandoned(true);

        connection = basicDataSource.getConnection();

        String sqlFormat = "Insert into test.test (trans_id,code,mobile) values (?,?,?)";
        statement = connection.prepareStatement(sqlFormat);
    }

    @Override
    public void invoke(MessageEvent value, Context context) throws Exception {
        // 每条事件都执行一次
        // 业务逻辑

        // 插入数据库
        statement.setString(1, value.getTransId());
        statement.setString(2, value.getCode());
        statement.setString(3, value.getMobile());
        statement.executeUpdate();
    }

    @Override
    public void finish() throws Exception {
        // 程序执行完成执行一次
        if (statement != null) {
            statement.close();
        }
        if (connection != null) {
            connection.close();
        }
    }
}
