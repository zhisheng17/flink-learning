package com.zhisheng.log.utils;

import com.zhisheng.common.utils.DateUtil;
import io.krakens.grok.api.Grok;
import io.krakens.grok.api.GrokCompiler;
import io.krakens.grok.api.Match;

import java.util.HashMap;
import java.util.Map;

import static com.zhisheng.common.utils.DateUtil.YYYY_MM_DD_HH_MM_SS;

/**
 * Desc: grok util
 * Created by zhisheng on 2019/10/26 下午11:50
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class GrokUtil {

    public static final GrokCompiler compiler = GrokCompiler.newInstance();
    public static Grok grok = null;

    public static Map<String, Object> toMap(String pattern, String message) {
        compiler.registerPatternFromClasspath("/patterns/patterns");
        grok = compiler.compile(pattern);
        if (grok != null) {
            Match match = grok.match(message);
            return match.capture();
        } else {
            return new HashMap<>();
        }
    }

    //test
    public static void main(String[] args) {
        String pattern = "%{KAFKALOG}";
        String message = "2019-10-26 19:53:05 INFO [GroupMetadataManager brokerId=0] Removed 0 expired offsets in 0 milliseconds. (kafka.coordinator.group.GroupMetadataManager)";
        Map<String, Object> messageMap = toMap(pattern, message);
        System.out.println(messageMap);
        System.out.println(messageMap.get("timestamp"));
        System.out.println(DateUtil.format(messageMap.get("timestamp").toString(), YYYY_MM_DD_HH_MM_SS));
    }
}