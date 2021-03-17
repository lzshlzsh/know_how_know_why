package architecture;


import architecture.bean.TestTable;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class Mysql2CK {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);
        tenv.executeSql("create table test (name string,age int,primary key (name) NOT ENFORCED)with('connector' = " +
                "'mysql-cdc','hostname' = '172.17.0.1','port' = '3306','username' = 'root','password' = " +
                "'1Dog@oicq','database-name' = 'test','table-name' = 'test','debezium.snapshot.mode' = 'initial')");
        Table table = tenv.sqlQuery("select name ,age from test");
        DataStream<Tuple2<Boolean, TestTable>> resultDS = tenv.toRetractStream(table, TestTable.class);
        SingleOutputStreamOperator<String> endDS = resultDS.map((MapFunction<Tuple2<Boolean, TestTable>, String>) value -> ((TestTable) value.f1).name + "  successful-test  " + ((TestTable) value.f1).age);
        endDS.print();
        tenv.executeSql("create table test01(name string,age bigint,   PRIMARY KEY (name) NOT ENFORCED)with(   'connector' =" +
                " " +
                "'clickhouse',    'url' = 'clickhouse://172.17.0.1:8123',    'database-name' = 'ck',    'table-name' =" +
                " 'test',    'sink.batch-size' = '1',          'sink.flush-interval' = '1000',      'sink.max-retries' = '3',            'sink.partition-strategy' = 'hash',    'sink.partition-key' = 'name',        'sink.ignore-delete' = 'true'    )");

        tenv.executeSql("create table test02(name string,age bigint,   PRIMARY KEY (name) NOT ENFORCED)with(   " +
                "'connector' =" +
                " " +
                "'clickhouse',    'url' = 'clickhouse://172.17.0.1:8123',    'database-name' = 'ck',    'table-name' =" +
                " 'test2',    'sink.batch-size' = '1',          'sink.flush-interval' = '1000',      'sink" +
                ".max-retries' = '3',            'sink.partition-strategy' = 'hash',    'sink.partition-key' = 'name',        'sink.ignore-delete' = 'true'    )");


        StatementSet statementSet = tenv.createStatementSet();
        statementSet.addInsertSql("insert into test01 select name , age from test");
        statementSet.addInsertSql("insert into test02 select name , age from test");
        statementSet.execute();

//        tenv.executeSql("insert into test01 select name , age from test");
//        tenv.executeSql("insert into test02 select name , age from test");
//        env.execute();
//        tenv.execute(Mysql2CK.class.getSimpleName());
    }
}
