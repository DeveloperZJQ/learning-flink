package com.happy.connectors.mysql;

import com.happy.common.model.TransInfo;;
import com.happy.common.utils.MySQLUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author happy
 * @create 2020-07-11 23:52
 */
public class SourceFromMySQL extends RichSourceFunction<TransInfo> {
    PreparedStatement ps;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtil.getConnection("com.mysql.jdbc.Driver",
                "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
                "root",
                "123456");
        String sql = "select * from transInfo;";
        ps = this.connection.prepareStatement(sql);
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) { //关闭连接和释放资源
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<TransInfo> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            TransInfo transInfo = new TransInfo (
                    resultSet.getLong("timeStamp"),
                    resultSet.getString("userId"),
                    resultSet.getString("cardId"),
                    resultSet.getString("transfer"),
                    resultSet.getString("fee"));
            ctx.collect(transInfo);
        }
    }

    @Override
    public void cancel() {
    }
}