package com.zhisheng.alert.model;

import com.zhisheng.common.constant.MachineConstant;
import com.zhisheng.common.model.MetricEvent;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

import static com.zhisheng.common.constant.MachineConstant.*;

/**
 * Desc: outage event
 * Created by zhisheng on 2019/10/15 上午12:11
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@NoArgsConstructor
public class OutageMetricEvent {
    //machine cluster name
    private String clusterName;
    //machine host ip
    private String hostIp;
    //event time
    private Long timestamp;
    //if the machine alert is recover (true/false)
    private Boolean recover;
    //the time when machine alert recover
    private Long recoverTime;
    //system time
    private Long systemTimestamp;
    //machine cpu usage percent
    private Double cpuUsePercent;
    //machine mem usage percent
    private Double memUsedPercent;
    //machine swap usage percent
    private Double swapUsedPercent;
    //machine load in five minutes
    private Double load5;
    //the machine alert count
    private int counter = 0;

    public OutageMetricEvent(String clusterName, String hostIp, long timestamp, Boolean recover, Long systemTimestamp) {
        this.clusterName = clusterName;
        this.hostIp = hostIp;
        this.timestamp = timestamp;
        this.recover = recover;
        this.systemTimestamp = systemTimestamp;
    }

    public String getKey() {
        return clusterName + hostIp;
    }

    public static OutageMetricEvent buildFromEvent(MetricEvent event) {
        Map<String, String> tags = event.getTags();
        Map<String, Object> fields = event.getFields();
        OutageMetricEvent outageMetricEvent = new OutageMetricEvent(tags.get(MachineConstant.CLUSTER_NAME),
                tags.get(MachineConstant.HOST_IP), event.getTimestamp(), null, null);

        switch (event.getName()) {
            case MEM:
                if (fields.containsKey(USED_PERCENT)) {
                    outageMetricEvent.setMemUsedPercent((Double) fields.get(USED_PERCENT));
                }
                break;
            case LOAD:
                if (fields.containsKey(LOAD5)) {
                    outageMetricEvent.setLoad5((Double) fields.get(LOAD5));
                }
                break;
            case SWAP:
                if (fields.containsKey(USED_PERCENT)) {
                    outageMetricEvent.setMemUsedPercent((Double) fields.get(USED_PERCENT));
                }
                break;
            case CPU:
                if (fields.containsKey(USED_PERCENT)) {
                    outageMetricEvent.setCpuUsePercent((Double) fields.get(USED_PERCENT));
                }
                break;
            default:
                return null;
        }
        return outageMetricEvent;
    }
}
