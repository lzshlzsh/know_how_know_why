package architecture;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.concurrent.TimeUnit;

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
                " -- 'source-offset-file' = 'mysql-bin.000022', \n" +
                " -- 'source-offset-pos' = '23549', \n" +
                " 'database-name' = 'test', \n" +
                " 'table-name' = 't_char', \n" +
                " 'source-pos-logging-interval' = '1 min'\n" +
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

        String sourceDDL2 = "CREATE TABLE t_char2 (\n" +
                " `aid` bigint, \n" +
                " `charname` string, \n" +
                " primary key (`aid`) not enforced \n" +
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc' ,\n" +
                " 'hostname' = '127.0.0.1', \n" +
                " 'port' = '3306', \n" +
                " 'username' = 'root', \n" +
                " 'password' = '1Dog@oicq', \n" +
                " -- 'source-offset-file' = 'mysql-bin.000005', \n" +
                " -- 'source-offset-pos' = '17181', \n" +
                " 'database-name' = 'test', \n" +
                " 'table-name' = 't_char2', \n" +
                " 'source-pos-logging-interval' = '1 min'\n" +
                ")";

        // mysql cdc sink
        String sinkDDL2 = "CREATE TABLE t_char_sink2 (\n" +
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
                " 'table-name' = 't_char_sink2' \n" +
                ")";

        // 创建执行环境
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        sEnv.setParallelism(2);
        sEnv.enableCheckpointing(30000);
        sEnv.disableOperatorChaining();
        sEnv.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        5, Time.of(5, TimeUnit.SECONDS), Time.of(5, TimeUnit.SECONDS)));

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, settings);

        //注册source和sink
        tEnv.executeSql(sourceDDL);
        tEnv.executeSql(sinkDDL);

        String sql = "INSERT INTO t_char_sink SELECT *,CURRENT_TIMESTAMP FROM t_char";

        String plan = tEnv.explainSql(sql);
        System.out.println(plan);

        tEnv.executeSql(sql);

        //注册source和sink
//        tEnv.executeSql(sourceDDL2);
//        tEnv.executeSql(sinkDDL2);
//
//        String sql2 = "INSERT INTO t_char_sink2 SELECT *,CURRENT_TIMESTAMP FROM t_char2";
//
//        String plan2 = tEnv.explainSql(sql2);
//        System.out.println(plan2);
//
//        tEnv.executeSql(sql2);

        tEnv.execute(MySQLCdc.class.getSimpleName());
    }
}
