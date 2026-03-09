package com.zhisheng.project.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 告警事件模型
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AlertEvent {
    /** 告警 ID */
    private String alertId;
    /** 告警级别：INFO, WARNING, CRITICAL */
    private String level;
    /** 告警规则名称 */
    private String ruleName;
    /** 告警消息 */
    private String message;
    /** 触发告警的指标名称 */
    private String metricName;
    /** 触发告警的指标值 */
    private Double metricValue;
    /** 告警阈值 */
    private Double threshold;
    /** 告警时间戳 */
    private Long timestamp;
    /** 告警来源主机 */
    private String host;
}
