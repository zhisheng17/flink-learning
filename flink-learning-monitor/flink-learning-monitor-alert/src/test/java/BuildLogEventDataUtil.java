import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.utils.GsonUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

/**
 * Desc: build log event data
 * Created by zhisheng on 2019/10/13 下午12:29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class BuildLogEventDataUtil {
    public static final String BROKER_LIST = "localhost:9092";
    public static final String LOG_TOPIC = "zhisheng_log";

    public static void writeDataToKafka() {
        Properties props = new Properties();
        props.put("bootstrap.servers", BROKER_LIST);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        for (int i = 0; i < 100; i++) {
            LogEvent logEvent = new LogEvent().builder()
                    .type("app")
                    .timestamp(System.currentTimeMillis())
                    .level(logLevel())
                    .message(message(i + 1))
                    .tags(mapData())
                    .build();
//            System.out.println(logEvent);
            ProducerRecord record = new ProducerRecord<String, String>(LOG_TOPIC, null, null, GsonUtil.toJson(logEvent));
            producer.send(record);
        }
        producer.flush();
    }

    public static void main(String[] args) {
        writeDataToKafka();
    }

    public static String message(int i) {
        return "这是第 " + i + " 行日志！";
    }

    public static String logLevel() {
        Random random = new Random();
        int number = random.nextInt(4);
        switch (number) {
            case 0:
                return "debug";
            case 1:
                return "info";
            case 2:
                return "warn";
            case 3:
                return "error";
            default:
                return "info";
        }
    }

    public static String hostIp() {
        Random random = new Random();
        int number = random.nextInt(4);
        switch (number) {
            case 0:
                return "121.12.17.10";
            case 1:
                return "121.12.17.11";
            case 2:
                return "121.12.17.12";
            case 3:
                return "121.12.17.13";
            default:
                return "121.12.17.10";
        }
    }

    public static Map<String, String> mapData() {
        Map<String, String> map = new HashMap<>();
        map.put("app_id", "11");
        map.put("app_name", "zhisheng");
        map.put("cluster_name", "zhisheng");
        map.put("host_ip", hostIp());
        map.put("class", "BuildLogEventDataUtil");
        map.put("method", "main");
        map.put("line", String.valueOf(new Random().nextInt(100)));
        //add more tag
        return map;
    }
}