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

//        tEnv.execute(MySQLCdc.class.getSimpleName());
    }
}
