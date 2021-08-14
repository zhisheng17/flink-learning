package com.zhisheng.connectors.hbase;

import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.METRICS_TOPIC;
import static com.zhisheng.connectors.hbase.constant.HBaseConstant.*;

/**
 * Desc: 读取流数据，然后写入到 HBase
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class HBaseStreamWriteMain {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        /*env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props))
                .writeUsingOutputFormat(new HBaseOutputFormat());*/


        DataStream<String> dataStream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 1L;

            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> out) throws Exception {
                while (isRunning) {
                    out.collect(String.valueOf(Math.floor(Math.random() * 100)));
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
            }
        });
        dataStream.writeUsingOutputFormat(new HBaseOutputFormat());


        env.execute("Flink HBase connector sink");
    }


    private static class HBaseOutputFormat implements OutputFormat<String> {

        private org.apache.hadoop.conf.Configuration configuration;
        private Connection connection = null;
        private String taskNumber = null;
        private Table table = null;
        private int rowNumber = 0;

        @Override
        public void configure(Configuration parameters) {
            configuration = HBaseConfiguration.create();
            configuration.set(HBASE_ZOOKEEPER_QUORUM, ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_ZOOKEEPER_QUORUM));
            configuration.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT, ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT));
            configuration.set(HBASE_RPC_TIMEOUT, ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_RPC_TIMEOUT));
            configuration.set(HBASE_CLIENT_OPERATION_TIMEOUT, ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_CLIENT_OPERATION_TIMEOUT));
            configuration.set(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD, ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_CLIENT_SCANNER_TIMEOUT_PERIOD));
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            connection = ConnectionFactory.createConnection(configuration);
            TableName tableName = TableName.valueOf(ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_TABLE_NAME));
            Admin admin = connection.getAdmin();
            if (!admin.tableExists(tableName)) { //检查是否有该表，如果没有，创建
                log.info("==============不存在表 = {}", tableName);
                admin.createTable(new HTableDescriptor(TableName.valueOf(ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_TABLE_NAME)))
                        .addFamily(new HColumnDescriptor(ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_COLUMN_NAME))));
            }
            table = connection.getTable(tableName);

            this.taskNumber = String.valueOf(taskNumber);
        }

        @Override
        public void writeRecord(String record) throws IOException {
            Put put = new Put(Bytes.toBytes(taskNumber + rowNumber));
            put.addColumn(Bytes.toBytes(ExecutionEnvUtil.PARAMETER_TOOL.get(HBASE_COLUMN_NAME)), Bytes.toBytes("zhisheng"),
                    Bytes.toBytes(String.valueOf(rowNumber)));
            rowNumber++;
            table.put(put);
        }

        @Override
        public void close() throws IOException {
            table.close();
            connection.close();
        }
    }
}
