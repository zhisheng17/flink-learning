package com.zhisheng.common.utils;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.net.URI;

import static com.zhisheng.common.constant.PropertiesConstants.*;
import static com.zhisheng.common.constant.PropertiesConstants.STREAM_CHECKPOINT_INTERVAL;

/**
 * Desc: Checkpoint 工具类
 * Created by zhisheng on 2019-09-06
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class CheckPointUtil {

    public static StreamExecutionEnvironment setCheckpointConfig(StreamExecutionEnvironment env, ParameterTool parameterTool) throws Exception{
        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_MEMORY.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            //1、state 存放在内存中，默认是 5M
            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_FS.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            StateBackend stateBackend = new FsStateBackend(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)), 0);
            env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));
            env.setStateBackend(stateBackend);
        }

        if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_ROCKETSDB.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(STREAM_CHECKPOINT_DIR));
            env.setStateBackend(rocksDBStateBackend);
        }

        //设置 checkpoint 周期时间
        env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000));

        //高级设置（这些配置也建议写成配置文件中去读取，优先环境变量）
        // 设置 exactly-once 模式
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 设置 checkpoint 最小间隔 500 ms
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // 设置 checkpoint 必须在1分钟内完成，否则会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        // 设置 checkpoint 失败时，任务不会 fail，该 checkpoint 会被丢弃
        env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        // 设置 checkpoint 的并发度为 1
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

        return env;
    }
}
