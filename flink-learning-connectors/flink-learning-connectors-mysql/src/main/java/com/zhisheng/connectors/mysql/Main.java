package com.zhisheng.connectors.mysql;


import com.google.common.collect.Lists;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import com.zhisheng.connectors.mysql.model.Student;
import com.zhisheng.connectors.mysql.sinks.SinkToMySQL;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.*;

/**
 * Created by zhisheng on 2019-02-17
 * Blog: http://www.54tianzhisheng.cn/2019/01/09/Flink-MySQL-sink/
 */
@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<Student> student = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props)).setParallelism(parameterTool.getInt(STREAM_PARALLELISM, 1))
                .map(string -> GsonUtil.fromJson(string, Student.class)).setParallelism(4); //解析字符串成 student 对象

        //timeWindowAll 并行度只能为 1
        student.timeWindowAll(Time.minutes(1)).apply(new AllWindowFunction<Student, List<Student>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<Student> values, Collector<List<Student>> out) throws Exception {
                ArrayList<Student> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    log.info("1 分钟内收集到 student 的数据条数是：" + students.size());
                    out.collect(students);
                }
            }
        }).addSink(new SinkToMySQL()).setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, 1));

        env.execute("flink learning connectors mysql");
    }
}
