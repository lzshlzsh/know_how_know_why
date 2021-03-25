package architecture.iceberg;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.data.RowData;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.source.FlinkSource;

public class StreamingSource {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .useBlinkPlanner()
                .build();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        TableLoader tableLoader = TableLoader.fromHadoopTable(Utils.TABLE_PATH);

        FlinkSource.forRowData()
                .env(env)
                .tableLoader(tableLoader)
                .streaming(false)
                .build().map(new MapFunction<RowData, String>() {
            @Override
            public String map(RowData value) throws Exception {
                return value.getRowKind().shortString() + "("
                        + value.getLong(0) + ","
                        + value.getString(1) + ","
                        + value.getLong(2)
                        + ")";
            }
        }).print();
        env.execute(StreamingSource.class.getSimpleName());
    }
}
