package sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class CanalCdc {
    public static void main(String[] args) throws Exception {
        // 为了方便测试，flink提供了自动生成数据的source.
        String sourceDDL = "CREATE TABLE kafka_bb_mtools_live_activity (\n" +
                "id                 bigint   ,\n" +
                "tenant_id          bigint   ,\n" +
                "title              string     ,\n" +
                "activity_status    smallint ,\n" +
                "cover_img          string     ,\n" +
                "ad                 string     ,\n" +
                "start_time         timestamp(3),\n" +
                "end_time           timestamp(3),\n" +
                "desc       string     ,\n" +
                "`extend`             string     ,\n" +
                "create_by          string     ,\n" +
                "create_by_name     string     ,\n" +
                "verify_by          string     ,\n" +
                "verify_by_name     string     ,\n" +
                "verify_desc        string     ,\n" +
                "created_at         timestamp(3),\n" +
                "updated_at         timestamp(3),\n" +
                "enter_num          bigint   ,\n" +
                "like_num           bigint   ,\n" +
                "comment_num        bigint   ,\n" +
                "p_tenant_id        bigint   ,\n" +
                "pid                bigint   ,\n" +
                "activity_type      smallint ,\n" +
                "display            smallint ,\n" +
                "follower_num       bigint   ,\n" +
                "join_num           bigint   ,\n" +
                "pool_id            string     ,\n" +
                "phase_id           string     ,\n" +
                "activity_sub_type  smallint ,\n" +
                "is_del             smallint ,\n" +
                " PRIMARY KEY (tenant_id,id) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'kafka-0.11',\n" +
                "    'topic' = 'canal',\n" +
                "    'scan.startup.mode' = 'latest-offset',\n" +
                "    'properties.bootstrap.servers' = '127.0.0.1:9092',\n" +
                "    'format' = 'canal-json',\n" +
                "    'properties.group.id' = 'testGroup'\n" +
                ")";

        // 为了方便测试，flink提供了控制台打印的print.
        String sinkDDL = "CREATE TABLE bb_mtools_live_activity_test \n" +
                "(\n" +
                " shard_key             string                        ,\n" +
                "id                 bigint   ,\n" +
                "tenant_id          bigint   ,\n" +
                "title              string     ,\n" +
                "activity_status    smallint ,\n" +
                "cover_img          string     ,\n" +
                "ad                 string     ,\n" +
                "start_time         timestamp(3),\n" +
                "end_time           timestamp(3),\n" +
                "desc       string     ,\n" +
                "`extend`             string     ,\n" +
                "create_by          string     ,\n" +
                "create_by_name     string     ,\n" +
                "verify_by          string     ,\n" +
                "verify_by_name     string     ,\n" +
                "verify_desc        string     ,\n" +
                "created_at         timestamp(3),\n" +
                "updated_at         timestamp(3),\n" +
                "enter_num          bigint   ,\n" +
                "like_num           bigint   ,\n" +
                "comment_num        bigint   ,\n" +
                "p_tenant_id        bigint   ,\n" +
                "pid                bigint   ,\n" +
                "activity_type      smallint ,\n" +
                "display            smallint ,\n" +
                "follower_num       bigint   ,\n" +
                "join_num           bigint   ,\n" +
                "pool_id            string     ,\n" +
                "phase_id           string     ,\n" +
                "activity_sub_type  smallint ,\n" +
                "is_del             smallint ,\n" +
                " PRIMARY KEY (id,shard_key) NOT ENFORCED\n" +
                ") WITH (\n" +
                "    'connector' = 'jdbc',\n" +
                "    'url' = 'jdbc:mysql://localhost:3307/live?characterEncoding=utf-8&useSSL=false',\n" +
                "    'table-name' = 'bb_mtools_live_activity_test',      -- 需要写入的数据表\n" +
                "    'username' = 'root',\n" +
                "    'password' = '123456',\n" +
                "    'lookup.cache.max-rows' = '1', -- 可选参数, 表示每批数据的最大缓存条数, 默认值是 5000\n" +
                "    'lookup.cache.ttl' = '1', -- 可选参数, 表示每批数据的刷新周期, 默认值是 0s\n" +
                "    'lookup.max-retries' = '3' -- 可选参数, 表示数据库写入出错时, 最多尝试的次数\n" +
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
        tEnv.sqlUpdate("insert into bb_mtools_live_activity_test select cast(a.tenant_id as string) shard_key, a.* from kafka_bb_mtools_live_activity a");

        //执行作业
        tEnv.execute("Flink Hello World");
    }
}
