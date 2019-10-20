package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.HashMap;
import java.util.Map;

/**
 * Desc: log event
 * Created by zhisheng on 2019/10/13 上午10:07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogEvent {
    //the type of log(app、docker、...)
    private String type;

    //the id of log
    private String id;

    // the timestamp of log
    private Long timestamp;

    //the level of log(debug/info/warn/error)
    private String level;

    //the offset of logfile
    private Long offset;

    //the content of offset log
    private String content;

    //the tag of log(appId、dockerId、machine hostIp、machine clusterName、...)
    private Map<String, String> tags = new HashMap<>();
}