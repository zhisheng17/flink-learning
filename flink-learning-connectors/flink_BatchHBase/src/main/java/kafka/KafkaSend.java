package kafka;

import com.alibaba.fastjson.JSON;
import model.Student;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 往kafka写数据
 */
public class KafkaSend {
    public static String broker_list = "localhost:9092";
    public static String topic = "student";

    public static void writeToKafka() throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer producer = new KafkaProducer<String, String>(props);
        for (int i = 1; i <= 100; i++) {
            Student student = new Student(i, "lightcity" + i, "pswd" + i, 18 + i);
            ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(student));
            producer.send(record);
            System.out.println("发送数据: " + JSON.toJSONString(student));  //自动调用student.toString()方法
            Thread.sleep(10*1000); //sleep 10s
        }
        producer.flush();
    }
    public static void main(String[] args) throws InterruptedException {
        writeToKafka();
    }
}
