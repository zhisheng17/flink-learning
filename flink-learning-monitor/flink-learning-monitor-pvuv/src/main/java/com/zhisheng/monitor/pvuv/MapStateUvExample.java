package com.zhisheng.monitor.pvuv;


import com.zhisheng.monitor.pvuv.model.UserVisitWebEvent;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.monitor.pvuv.utils.UvExampleUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @author fanrui
 * @date 2019-10-26 19:46:48
 * @desc 使用 MapState 来维护访问过网站各页面的 用户id
 */
public class MapStateUvExample {
    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1));
        env.setParallelism(2);

        CheckpointConfig checkpointConf = env.getCheckpointConfig();
        checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, UvExampleUtil.broker_list);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "app-uv-stat");

        FlinkKafkaConsumerBase<String> kafkaConsumer = new FlinkKafkaConsumer011<>(
                UvExampleUtil.topic, new SimpleStringSchema(), props)
                .setStartFromGroupOffsets();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig
                .Builder().setHost("192.168.30.244").build();

        env.addSource(kafkaConsumer)
            .map(string -> GsonUtil.fromJson(string, UserVisitWebEvent.class))  // 反序列化 JSON
            .keyBy("date","pageId") // 按照 日期和页面 进行 keyBy
            .map(new RichMapFunction<UserVisitWebEvent, Tuple2<String, Long>>() {
                // 存储当前 key 对应的 userId 集合
                private MapState<String,Boolean> userIdState;
                // 存储当前 key 对应的 UV 值
                private ValueState<Long> uvState;

                @Override
                public Tuple2<String, Long> map(UserVisitWebEvent userVisitWebEvent) throws Exception {
                    // 初始化 uvState
                    if(null == uvState.value()){
                        uvState.update(0L);
                    }
                    // userIdState 中不包含当前访问的 userId，说明该用户今天还未访问过该页面
                    // 则将该 userId put 到 userIdState 中，并把 UV 值 +1
                    if(!userIdState.contains(userVisitWebEvent.getUserId())){
                        userIdState.put(userVisitWebEvent.getUserId(),null);
                        uvState.update(uvState.value() + 1);
                    }
                    // 生成 Redis key，格式为 日期_pageId，如: 20191026_0
                    String redisKey = userVisitWebEvent.getDate() + "_"
                            + userVisitWebEvent.getPageId();
                    System.out.println(redisKey + "   :::   " + uvState.value());
                    return Tuple2.of(redisKey, uvState.value());
                }

                @Override
                public void open(Configuration parameters) throws Exception {
                    super.open(parameters);
                    // 从状态中恢复 userIdState
                    userIdState = getRuntimeContext().getMapState(
                            new MapStateDescriptor<>("userIdState",
                                    TypeInformation.of(new TypeHint<String>() {}),
                                    TypeInformation.of(new TypeHint<Boolean>() {})));
                    // 从状态中恢复 uvState
                    uvState = getRuntimeContext().getState(
                            new ValueStateDescriptor<>("uvState",
                                    TypeInformation.of(new TypeHint<Long>() {})));
                }
            })
            .addSink(new RedisSink<>(conf, new RedisSetSinkMapper()));

        env.execute("Redis Set UV Stat");
    }

    // 数据与 Redis key 的映射关系，并指定将数据 set 到 Redis
    public static class RedisSetSinkMapper
            implements RedisMapper<Tuple2<String, Long>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            // 这里必须是 set 操作，通过 MapState 来维护用户集合，
            // 输出到 Redis 仅仅是为了展示结果供其他系统查询统计结果
            return new RedisCommandDescription(RedisCommand.SET);
        }

        @Override
        public String getKeyFromData(Tuple2<String, Long> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, Long> data) {
            return data.f1.toString();
        }
    }
}