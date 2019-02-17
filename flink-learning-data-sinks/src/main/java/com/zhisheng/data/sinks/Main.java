package com.zhisheng.data.sinks;


import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.data.sinks.model.Student;
import com.zhisheng.data.sinks.sinks.SinkToMySQL;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class Main {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("zookeeper.connect", "localhost:2181");
        props.put("group.id", "metric-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "latest");

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                "student",   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(1)
                .map(string -> GsonUtil.fromJson(string, Student.class)); //博客里面用的是 fastjson，这里用的是gson解析，解析字符串成 student 对象

        student.addSink(new SinkToMySQL()); //数据 sink 到 mysql

        env.execute("Flink data sink");
    }
}
