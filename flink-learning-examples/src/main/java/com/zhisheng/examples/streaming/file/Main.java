package com.zhisheng.examples.streaming.file;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 从文件读取数据 & 数据写入到文件
 *  blog：http://www.54tianzhisheng.cn/
 *  微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> data = env.readTextFile("file:///usr/local/blink-1.5.1/README.txt");
        data.print();

        //两种格式都行，另外还支持写入到 hdfs
//        data.writeAsText("file:///usr/local/blink-1.5.1/README1.txt");
        data.writeAsText("/usr/local/blink-1.5.1/README1.txt");

        env.execute();
    }
}
