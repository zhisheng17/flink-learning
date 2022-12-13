import com.zhisheng.log.util.ExceptionUtil;

import java.util.HashMap;
import java.util.Map;

public class ExceptionUtilTest {


    public static void main(String[] args) {

        Throwable throwable = new Throwable("producer the metrics to kafka has exception\n" +
                "java.util.ConcurrentModificationException: null\n" +
                "\tat java.util.HashMap$HashIterator.nextNode(HashMap.java:1445)\n" +
                "\tat java.util.HashMap$EntryIterator.next(HashMap.java:1479)\n" +
                "\tat java.util.HashMap$EntryIterator.next(HashMap.java:1477)\n" +
                "\tat org.apache.flink.metrics.kafka.KafkaReporter.report(KafkaReporter.java:220)\n" +
                "\tat org.apache.flink.runtime.metrics.MetricRegistryImpl$ReporterTask.run(MetricRegistryImpl.java:451)\n" +
                "\tat java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:511)\n" +
                "\tat java.util.concurrent.FutureTask.runAndReset(FutureTask.java:308)\n" +
                "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.access$301(ScheduledThreadPoolExecutor.java:180)\n" +
                "\tat java.util.concurrent.ScheduledThreadPoolExecutor$ScheduledFutureTask.run(ScheduledThreadPoolExecutor.java:294)\n" +
                "\tat java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1149)\n" +
                "\tat java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:624)");

        String stacktrace = ExceptionUtil.stacktraceToString(throwable);
        System.out.println(stacktrace);


        Map<String, String> map = new HashMap<>();
        

        for (Map.Entry<String, String> entry : map.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }

    }

}
