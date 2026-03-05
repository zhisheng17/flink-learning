package com.zhisheng.project.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 交易事件模型
 * 用于实时风控系统的交易数据
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TransactionEvent {
    /** 交易 ID */
    private String transactionId;
    /** 用户 ID */
    private String userId;
    /** 交易金额 */
    private Double amount;
    /** 交易类型：TRANSFER(转账), PAYMENT(支付), WITHDRAW(提现) */
    private String transactionType;
    /** 来源账户 */
    private String sourceAccount;
    /** 目标账户 */
    private String targetAccount;
    /** 交易发生的 IP 地址 */
    private String ipAddress;
    /** 交易发生的城市 */
    private String city;
    /** 交易时间戳 */
    private Long timestamp;
    /** 设备指纹 */
    private String deviceFingerprint;
}
