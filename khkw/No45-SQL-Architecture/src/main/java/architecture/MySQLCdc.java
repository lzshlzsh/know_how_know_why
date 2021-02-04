package architecture;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MySQLCdc {
    public static void main(String[] args) throws Exception {
        // mysql cdc source
        String sourceDDL = "CREATE TABLE t_char (\n" +
                " `aid` bigint, \n" +
                " `charname` string, \n" +
                " primary key (`aid`) not enforced \n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc' ,\n" +
                " 'hostname' = '127.0.0.1', \n" +
                " 'port' = '3306', \n" +
                " 'username' = 'root', \n" +
                " 'password' = '1Dog@oicq', \n" +
                " 'database-name' = 'test', \n" +
                " 'table-name' = 't_char' \n" +
                ")";

        // mysql cdc sink
        String sinkDDL = "CREATE TABLE t_char_sink (\n" +
                " `aid` bigint, \n" +
                " `charname` string, \n" +
                " `timestamp` TIMESTAMP, \n" +
                " primary key (`aid`) not enforced \n" +
                ") WITH (\n" +
                " 'connector' = 'jdbc', \n" +
                " 'driver' = 'com.mysql.cj.jdbc.Driver', \n" +
                " 'url' = 'jdbc:mysql://127.0.0.1:3306/test', \n" +
                " 'username' = 'root', \n" +
                " 'password' = '1Dog@oicq', \n" +
                " 'table-name' = 't_char_sink' \n" +
                ")";


        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        String sql = "INSERT INTO t_char_sink SELECT *,CURRENT_TIMESTAMP FROM t_char";

        String plan = tEnv.explainSql(sql);
        System.out.println(plan);

        tEnv.executeSql(sql);
    }
}
