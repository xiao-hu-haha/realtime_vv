package com.dwd;

import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.SQLUtil;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;

import java.util.Date;

public class RealtimeDwdInteractionCommentInfo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
        StreamTableEnvironment tenv  = StreamTableEnvironment.create(env);
//        tenv.executeSql("CREATE TABLE topic_db (\n" +
//                " op string," +
//                "db string," +
//                "before map<String,String>," +
//                "after map<String,String>," +
//                "source map<String,String>," +
//                "ts_ms bigint," +
//                "proc_time as proctime(),\n" +
//                "row_time as TO_TIMESTAMP_LTZ(ts_ms,3)," +
//                "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND" +
//                ")\n" +
//                "  'connector' = 'kafka',\n" +
//                "  'topic' = '" + "topic_db" + "',\n" +
//                "  'properties.bootstrap.servers' = '" + "cdh01:9092,cdh02:9092,cdh03:9092" + "',\n" +
//                "  'properties.group.id' = 'fsf',\n" +
//                "  'scan.startup.mode' = 'earliest-offset',\n" +
//                "  'format' = 'json'\n" +
//                ")");
//        Table table = tenv.sqlQuery("select * from topic_db");
//        tenv.toDataStream(table).print();
        Table table = tenv.sqlQuery("CREATE TABLE topic_db (\n" +
                "  `before` Map<string,string>,\n" +
                "  `after` Map<string,string>,\n" +
                "  `op` STRING,\n" +
                "  `ts_ms` BIGINT,\n" +
                "   proc_time as proctime(),\n" +
                "  row_time as TO_TIMESTAMP_LTZ(ts_ms ,3) ,\n" +
                "  WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND\n" +
                ")" + SQLUtil.getKafkaSourceSQL(ConfigUtils.getString("kafka.topic.db"), new Date().toString())
        );
        tenv.createTemporaryView("topic_db",table);
        tenv.sqlQuery("select * from  topic_db").execute().print();

        env.execute();
    }
}
