package com.zhisheng.examples.streaming.restartStrategy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

import static com.zhisheng.common.constant.PropertiesConstants.STREAM_CHECKPOINT_INTERVAL;

/**
 * Desc: NullPointerException application，default RestartStrategy enable checkpoint Test
 * Created by zhisheng on 2019/10/5 下午11:22
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class EnableCheckpointMain {
    public static void main(String[] args) throws Exception {
        //创建流运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(ParameterTool.fromArgs(args));
        env.setParallelism(1);

        env.addSource(new SourceFunction<Long>() {
            @Override
            public void run(SourceContext<Long> sourceContext) throws Exception {
                while (true) {
                    sourceContext.collect(null);
                }
            }
            @Override
            public void cancel() {
            }
        })
                .map((MapFunction<Long, Long>) aLong -> aLong / 1)
                .print();

        //开启 checkpoint
        StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
        env.enableCheckpointing(10000);
        env.setStateBackend(stateBackend);

        env.execute("zhisheng default RestartStrategy enable checkpoint example");
    }
}
