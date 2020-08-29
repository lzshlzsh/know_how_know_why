package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class MyApp {
    public static void main(String[] args) throws Exception {
        // 为了方便测试，flink提供了自动生成数据的source.
        String sourceDDL = "CREATE TABLE random_source (\n" +
                " -- f_sequence INT,\n" +
                " f_random INT,\n" +
                " f_random_str STRING\n" +
                ") WITH (\n" +
                " 'connector' = 'datagen',\n" +
                " 'rows-per-second'='1',\n" +
                " -- 'fields.f_sequence.kind'='sequence',\n" +
                " -- 'fields.f_sequence.start'='1',\n" +
                " -- 'fields.f_sequence.end'='1000',\n" +
                " 'fields.f_random.min'='1', -- test\n" +
                " 'fields.f_random.max'='1000',\n" +
                " 'fields.f_random_str.length'='10'\n" +
                ")";

        // 为了方便测试，flink提供了控制台打印的print.
        String sinkDDL = "CREATE TABLE print_sink (\n" +
                " f_count BIGINT\n" +
                ") WITH (\n" +
                " 'connector' = 'print'\n" +
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
        tEnv.sqlUpdate("INSERT INTO print_sink SELECT COUNT(1) AS f_count FROM random_source");

        //执行作业
        tEnv.execute("Flink Hello World");
    }
}
