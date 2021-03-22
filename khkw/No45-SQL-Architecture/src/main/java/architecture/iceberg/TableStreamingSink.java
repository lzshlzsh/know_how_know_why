package architecture.iceberg;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TableStreamingSink {
    public static void main(String[] args) {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        tEnv.executeSql("CREATE TABLE `default_catalog`.`default_database`.`source` (\n" +
                " `aid` bigint, \n" +
                " `charname` string, \n" +
                " `tm` bigint,\n" +
                "  primary key (`aid`) not enforced\n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc' ,\n" +
                " 'hostname' = '172.17.0.1', \n" +
                " 'port' = '3306', \n" +
                " 'username' = 'root', \n" +
                " 'password' = '1Dog@oicq', \n" +
                " -- 'source-offset-file' = 'mysql-bin.000022', \n" +
                " -- 'source-offset-pos' = '23549', \n" +
                " 'database-name' = 'test', \n" +
                " 'table-name' = 'iceberg_source', \n" +
                " 'source-pos-logging-interval' = '1 min'\n" +
                ")");

        tEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs:///iceberg',\n" +
                "  'property-version'='1'\n" +
                ")");
        tEnv.executeSql("use catalog hadoop_catalog");
        tEnv.executeSql("drop table if exists sample");

        // table api不支持upser
        tEnv.executeSql("CREATE TABLE sample (\n" +
                "    id BIGINT,\n" +
                "    data STRING,\n" +
                "    tm BIGINT\n" +
                "    -- primary key (`id`) not enforced\n" +
                ") with (\n" +
                "'write.parquet.row-group-size-bytes' = '1048576'\n" +
                ")");
        tEnv.executeSql("insert into sample select * from `default_catalog`.`default_database`.`source`");
    }
}
