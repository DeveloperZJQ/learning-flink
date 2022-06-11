package com.flink.tableandsql;

import org.apache.flink.connector.datagen.table.DataGenConnectorOptions;
import org.apache.flink.table.api.*;

/**
 * @author happy
 * @since 2022/6/10
 */
public class TableAndSQLFirstProgrammer {
    public static void main(String[] args) {
        final EnvironmentSettings settings =
                EnvironmentSettings.newInstance().inStreamingMode().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

// Create a source table
        tableEnv.createTemporaryTable("SourceTable", TableDescriptor.forConnector("datagen")
                .schema(Schema.newBuilder()
                        .column("f0", DataTypes.STRING())
                        .build())
                .option(DataGenConnectorOptions.ROWS_PER_SECOND, 100L)
                .build());

// Create a sink table (using SQL DDL)
        tableEnv.executeSql("CREATE TEMPORARY TABLE SinkTable WITH ('connector' = 'blackhole') LIKE SourceTable (EXCLUDING ALL)");

// Create a Table object from a Table API query
        Table table2 = tableEnv.from("SourceTable");

// Create a Table object from a SQL query
        tableEnv.sqlQuery("SELECT * FROM SourceTable").execute().print();

        tableEnv.createTemporaryView("TEMP_TABLE_A", table2);
        tableEnv.sqlQuery("SELECT * FROM TEMP_TABLE_A").execute().print();
// Emit a Table API result Table to a TableSink, same for SQL result
        table2.insertInto("SinkTable").execute();
    }
}
