package com.zhisheng.project.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 告警规则模型
 * 用于动态告警规则的广播状态
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class AlertRule {
    /** 规则 ID */
    private String ruleId;
    /** 规则名称 */
    private String ruleName;
    /** 监控指标名称 */
    private String metricName;
    /** 比较操作符：GT(大于), LT(小于), GTE(大于等于), LTE(小于等于) */
    private String operator;
    /** 告警阈值 */
    private Double threshold;
    /** 告警级别 */
    private String alertLevel;
    /** 规则是否启用 */
    private Boolean enabled;
    /** 窗口大小（秒） */
    private Integer windowSizeSeconds;
}
