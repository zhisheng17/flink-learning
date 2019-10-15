package com.zhisheng.alert.model;

import com.zhisheng.common.model.MetricEvent;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: alert event
 * Created by zhisheng on 2019/10/13 上午10:14
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class AlertEvent {

    private String type;

    private MetricEvent metricEvent;

    private boolean recover;

    private Long trigerTime;

    private Long recoverTime;
}