package com.zhisheng.alert.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: alert rule
 * Created by zhisheng on 2019/10/16 下午5:07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class AlertRule {
    private Integer id;
    private String name;
    private String measurement;
    private String thresholds;
}