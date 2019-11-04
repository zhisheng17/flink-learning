import com.zhisheng.common.model.LogEvent;
import com.zhisheng.common.utils.GsonUtil;

import java.util.HashMap;
import java.util.Map;

/**
 * Desc: log event data example
 * Created by zhisheng on 2019/10/13 下午12:29
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class LogEventDataExample {
    public static void main(String[] args) {

        LogEvent logEvent = new LogEvent();
        logEvent.setType("app");
        Map<String, String> tags = new HashMap<>();
        tags.put("cluster_name", "zhisheng");
        tags.put("host_ip", "127.0.0.1");
        tags.put("app_id", "21");
        tags.put("app_name", "zhisheng");


        String message = "Exception in thread \"main\" java.lang.NoClassDefFoundError: org/apache/flink/api/common/ExecutionConfig$GlobalJobParameters";


        LogEvent event = new LogEvent().builder()
                .type("app")
                .timestamp(System.currentTimeMillis())
                .level("error")
                .message(message)
                .tags(tags).build();

        System.out.println(GsonUtil.toJson(event));
    }

}
