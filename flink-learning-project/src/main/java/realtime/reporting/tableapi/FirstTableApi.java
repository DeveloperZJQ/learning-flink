package realtime.reporting.tableapi;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;

/**
 * @author DeveloperZJQ
 * @since 2022-3-29
 */
public class FirstTableApi {
    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("input param invalid");
            return;
        }
        String ip = args[0];
        EnvironmentSettings mode = EnvironmentSettings.inBatchMode();
        TableEnvironment tableEnvironment = TableEnvironment.create(mode);

        tableEnvironment.executeSql("CREATE TABLE transactions (\n" +
                "    account_id  BIGINT,\n" +
                "    amount      BIGINT,\n" +
                "    transaction_time TIMESTAMP(3),\n" +
                "    WATERMARK FOR transaction_time AS transaction_time - INTERVAL '5' SECOND\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka',\n" +
                "    'topic'     = 'transactions',\n" +
                "    'properties.bootstrap.servers' = '" + ip + ":9092',\n" +
                "    'format'    = 'csv'\n" +
                ")");

        tableEnvironment.executeSql("CREATE TABLE spend_report (\n" +
                "    account_id BIGINT,\n" +
                "    log_ts     TIMESTAMP(3),\n" +
                "    amount     BIGINT\n," +
                "    PRIMARY KEY (account_id, log_ts) NOT ENFORCED" +
                ") WITH (\n" +
                "   'connector'  = 'jdbc',\n" +
                "   'url'        = 'jdbc:mysql://" + ip + ":3306/flink_sql',\n" +
                "   'table-name' = 'spend_report',\n" +
                "   'driver'     = 'com.mysql.jdbc.Driver',\n" +
                "   'username'   = 'root',\n" +
                "   'password'   = '123456'\n" +
                ")");

        Table transactions = tableEnvironment.from("transactions");
        transactions.executeInsert("spend_report");
    }
}