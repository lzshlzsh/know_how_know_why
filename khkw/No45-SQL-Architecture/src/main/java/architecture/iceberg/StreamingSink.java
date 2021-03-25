package architecture.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.iceberg.DistributionMode;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;


public class StreamingSink {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        env.enableCheckpointing(30_000);

        Utils.createCdcSource(tEnv);
        Utils.createHadoopCatalog(tEnv);
        Utils.createHadoopSinkV2(tEnv);

        Table sourceTable = tEnv.sqlQuery("select * from source");
        DataStream<Row> source = tEnv
                .toRetractStream(sourceTable, Row.class)
                .map((MapFunction<Tuple2<Boolean, Row>, Row>) value -> value.f1)
                .keyBy(r -> r.getField(0));

        TableLoader tableLoader = TableLoader.fromHadoopTable(Utils.TABLE_PATH);
        FlinkSink.forRow(source, Utils.TABLE_SCHEMA)
                .tableLoader(tableLoader)
                .distributionMode(DistributionMode.HASH)
                .equalityFieldColumns(ImmutableList.of("id"))
                .build();

        env.execute(StreamingSink.class.getSimpleName());
    }
}
