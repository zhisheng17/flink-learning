import com.zhisheng.common.model.MetricEvent;
import com.zhisheng.common.utils.GsonUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.*;

/**
 * Desc: build machine metric data
 * Created by zhisheng on 2019/10/15 上午10:47
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class BuildMachineMetricDataUtil {

    public static final String BROKER_LIST = "localhost:9092";
    public static final String METRICS_TOPIC = "zhisheng_metrics";
    public static Random random = new Random();

    public static List<String> hostIps = Arrays.asList("121.12.17.10", "121.12.17.11", "121.12.17.12", "121.12.17.13");

    public static void writeDataToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        while (true) {
            long timestamp = System.currentTimeMillis();
            for (int i = 0; i < hostIps.size(); i++) {
                MetricEvent cpuData = buildCpuData(hostIps.get(i), timestamp);
                MetricEvent loadData = buildLoadData(hostIps.get(i), timestamp);
                MetricEvent memData = buildMemData(hostIps.get(i), timestamp);
                MetricEvent swapData = buildSwapData(hostIps.get(i), timestamp);
                ProducerRecord cpuRecord = new ProducerRecord<String, String>(METRICS_TOPIC, null, null, GsonUtil.toJson(cpuData));
                ProducerRecord loadRecord = new ProducerRecord<String, String>(METRICS_TOPIC, null, null, GsonUtil.toJson(loadData));
                ProducerRecord memRecord = new ProducerRecord<String, String>(METRICS_TOPIC, null, null, GsonUtil.toJson(memData));
                ProducerRecord swapRecord = new ProducerRecord<String, String>(METRICS_TOPIC, null, null, GsonUtil.toJson(swapData));

                producer.send(cpuRecord);
                producer.send(loadRecord);
//                System.out.println(cpuData);
//                System.out.println(loadData);
                producer.send(memRecord);
                producer.send(swapRecord);
            }
            producer.flush();
            Thread.sleep(1000);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        writeDataToKafka();
    }

    public static MetricEvent buildCpuData(String hostIp, Long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        int used = random.nextInt(2048);
        int max = 2048;
        metricEvent.setName("cpu");
        metricEvent.setTimestamp(timestamp);
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", hostIp);
        fields.put("usedPercent", (double) used / max * 100);
        fields.put("used", used);
        fields.put("max", max);
        metricEvent.setFields(fields);
        metricEvent.setTags(tags);
        return metricEvent;
    }

    public static MetricEvent buildLoadData(String hostIp, Long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        metricEvent.setName("load");
        metricEvent.setTimestamp(timestamp);
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", hostIp);
        fields.put("load1", random.nextInt(100));
        fields.put("load5", random.nextInt(50));
        fields.put("load15", random.nextInt(25));
        metricEvent.setFields(fields);
        metricEvent.setTags(tags);
        return metricEvent;
    }

    public static MetricEvent buildSwapData(String hostIp, Long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        int used = random.nextInt(1024);
        int max = 1024;
        metricEvent.setName("swap");
        metricEvent.setTimestamp(timestamp);
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", hostIp);
        fields.put("usedPercent", (double) used / max * 100);
        fields.put("used", used);
        fields.put("max", max);
        metricEvent.setFields(fields);
        metricEvent.setTags(tags);
        return metricEvent;
    }

    public static MetricEvent buildMemData(String hostIp, Long timestamp) {
        MetricEvent metricEvent = new MetricEvent();
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        int used = random.nextInt(2048);
        int max = 2048;
        metricEvent.setName("mem");
        metricEvent.setTimestamp(timestamp);
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", hostIp);
        fields.put("usedPercent", (double) used / max * 100);
        fields.put("used", used);
        fields.put("max", max);
        metricEvent.setFields(fields);
        metricEvent.setTags(tags);
        return metricEvent;
    }
}