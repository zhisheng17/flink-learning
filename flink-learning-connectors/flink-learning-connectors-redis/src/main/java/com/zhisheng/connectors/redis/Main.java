package com.zhisheng.connectors.redis;


import com.zhisheng.common.model.ProductEvent;
import com.zhisheng.common.utils.ExecutionEnvUtil;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.KafkaConfigUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisSentinelConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.net.InetSocketAddress;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

import static com.zhisheng.common.constant.PropertiesConstants.METRICS_TOPIC;

/**
 * Desc: 从 Kafka 中读取数据然后写入到 Redis
 * Created by zhisheng on 2019-04-29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool parameterTool = ExecutionEnvUtil.PARAMETER_TOOL;
        Properties props = KafkaConfigUtil.buildKafkaProps(parameterTool);

        SingleOutputStreamOperator<Tuple2<String, String>> product = env.addSource(new FlinkKafkaConsumer011<>(
                parameterTool.get(METRICS_TOPIC),   //这个 kafka topic 需要和上面的工具类的 topic 一致
                new SimpleStringSchema(),
                props))
                .map(string -> GsonUtil.fromJson(string, ProductEvent.class)) //反序列化 JSON
                .flatMap(new FlatMapFunction<ProductEvent, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(ProductEvent value, Collector<Tuple2<String, String>> out) throws Exception {
                        //收集商品 id 和 price 两个属性
                        out.collect(new Tuple2<>(value.getId().toString(), value.getPrice().toString()));
                    }
                });
//        product.print();

        //单个 Redis
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(parameterTool.get("redis.host")).build();
        product.addSink(new RedisSink<Tuple2<String, String>>(conf, new RedisSinkMapper()));

        //Redis 的 ip 信息一般都从配置文件取出来
        //Redis 集群
/*        FlinkJedisClusterConfig clusterConfig = new FlinkJedisClusterConfig.Builder()
                .setNodes(new HashSet<InetSocketAddress>(
                        Arrays.asList(new InetSocketAddress("redis1", 6379)))).build();*/

        //Redis Sentinels
/*        FlinkJedisSentinelConfig sentinelConfig = new FlinkJedisSentinelConfig.Builder()
                .setMasterName("master")
                .setSentinels(new HashSet<>(Arrays.asList("sentinel1", "sentinel2")))
                .setPassword("")
                .setDatabase(1).build();*/

        env.execute("flink redis connector");
    }


    public static class RedisSinkMapper implements RedisMapper<Tuple2<String, String>> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "zhisheng");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }
}