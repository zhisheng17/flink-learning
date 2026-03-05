package com.zhisheng.project.risk.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 风险事件模型
 * 风控系统检测到异常时输出的事件
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class RiskEvent {
    /** 风险事件 ID */
    private String riskId;
    /** 用户 ID */
    private String userId;
    /** 风险类型：FRAUD(欺诈), SUSPICIOUS(可疑), HIGH_FREQUENCY(高频) */
    private String riskType;
    /** 风险等级：LOW, MEDIUM, HIGH */
    private String riskLevel;
    /** 风险描述 */
    private String description;
    /** 触发风险的交易金额 */
    private Double amount;
    /** 检测时间戳 */
    private Long timestamp;
    /** 关联的交易 ID 列表（逗号分隔） */
    private String relatedTransactionIds;
}
