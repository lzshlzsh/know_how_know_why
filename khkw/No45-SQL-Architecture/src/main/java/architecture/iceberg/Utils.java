package architecture.iceberg;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopTables;
import org.apache.iceberg.types.Types;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class Utils {
    public static final String WAREHOUSE_PATH = "hdfs://cluster-endpoint:9000/iceberg";
    public static final String TABLE_PATH = WAREHOUSE_PATH + "/default/sample";

    public static final Schema SCHEMA = new Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "data", Types.StringType.get()),
            Types.NestedField.optional(3, "tm", Types.LongType.get())
    );

    public static final TableSchema TABLE_SCHEMA = TableSchema.builder()
            .field("id", DataTypes.BIGINT())
            .field("data", DataTypes.STRING())
            .field("tm", DataTypes.BIGINT())
            .build();

    public static void createCdcSource(StreamTableEnvironment tEnv) {
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

    public static void createHadoopCatalog(StreamTableEnvironment tEnv) {
        tEnv.executeSql("CREATE CATALOG hadoop_catalog WITH (\n" +
                "  'type'='iceberg',\n" +
                "  'catalog-type'='hadoop',\n" +
                "  'warehouse'='" + WAREHOUSE_PATH + "',\n" +
                "  'property-version'='1'\n" +
                ")");
    }

    public static void createHadoopSinkV1(StreamTableEnvironment tEnv) {
        HadoopTables hadoopTables = new HadoopTables();
        if (!hadoopTables.exists(TABLE_PATH)) {
            tEnv.executeSql("CREATE TABLE `hadoop_catalog`.`default`.`sample` (\n" +
                    "    id BIGINT COMMENT 'unique id',\n" +
                    "    data STRING,\n" +
                    "    tm BIGINT\n" +
                    ") with (\n" +
                    "'write.parquet.row-group-size-bytes' = '1048576'\n" +
                    ")");
        }
    }

    public static void createHadoopSinkV2(StreamTableEnvironment tEnv) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException, ClassNotFoundException {
        HadoopTables hadoopTables = new HadoopTables();
        Table table;
        if (!hadoopTables.exists(TABLE_PATH)) {
            table = hadoopTables.buildTable(TABLE_PATH, SCHEMA)
                    .withLocation(TABLE_PATH)
                    .withPartitionSpec(PartitionSpec.builderFor(SCHEMA).identity("data").build())
                    .withProperty("write.parquet.row-group-size-bytes", "1048576")
                    .withProperty("commit.manifest.min-count-to-merge", "10")
                    .withProperty("history.expire.max-snapshot-age-ms", "300000")
                    .withProperty("write.distribution-mode", "hash")
                    .create();
        } else {
            table = hadoopTables.load(TABLE_PATH);
        }
        // 解决小文件和数据热点问题
        table.updateSpec().addField(Expressions.bucket("id", 32)).commit();

        // 开启v2格式table，参考 https://iceberg.apache.org/spec/
        BaseTable baseTable = ((BaseTable) table);
        TableMetadata tableMetadata = baseTable.operations().current();

        Method method = TableMetadata.class.getDeclaredMethod(
                "newTableMetadata",
                Schema.class,
                PartitionSpec.class,
                SortOrder.class,
                String.class,
                Map.class,
                int.class);
        method.setAccessible(true);
        TableMetadata v2TableMetadata = (TableMetadata) method.invoke(null,
                tableMetadata.schema(),
                tableMetadata.spec(),
                tableMetadata.sortOrder(),
                tableMetadata.location(),
                tableMetadata.properties(),
                2
        );
        baseTable.operations().commit(tableMetadata, v2TableMetadata);
    }
}
