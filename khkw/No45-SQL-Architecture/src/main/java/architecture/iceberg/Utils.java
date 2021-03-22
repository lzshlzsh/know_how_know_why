package architecture.iceberg;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Utils {
    public static final TableSchema SINK_SCHEMA = TableSchema.builder()
            .field("id", DataTypes.BIGINT())
            .field("data", DataTypes.STRING())
            .field("tm", DataTypes.BIGINT())
            .build();

    public static void CreateCdcSource(StreamTableEnvironment tEnv) {
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
    }

    public static void CreateHadoopSink(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='hdfs:///iceberg',\n" +
                "  'property-version'='1'\n" +
                ")");
        tEnv.executeSql("drop table if exists `hadoop_catalog`.`default`.`sample`");
        tEnv.executeSql("CREATE TABLE `hadoop_catalog`.`default`.`sample` (\n" +
                "    id BIGINT COMMENT 'unique id',\n" +
                "    data STRING,\n" +
                "    tm BIGINT\n" +
                ") with (\n" +
                "'write.parquet.row-group-size-bytes' = '1048576'\n" +
                ")");
    }
}
