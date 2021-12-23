package com.zhisheng.data.sinks;

import com.zhisheng.data.sinks.sinks.MySink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * Desc: test RichSink function construction method and open function
 * https://t.zsxq.com/EIiyjeU
 * Created by zhisheng on 2019-09-26
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main2 {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("127.0.0.1", 9000);
        source.addSink(new MySink("6")).setParallelism(5);
        env.execute("xxxx");
    }
}
