package com.zhisheng.project.warehouse.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DWS 层 - 订单统计汇总
 * 按类别和时间窗口的订单聚合统计
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderStats {
    /** 商品类别 */
    private String category;
    /** 订单数量 */
    private Long orderCount;
    /** 商品总数量 */
    private Long totalQuantity;
    /** 总销售金额 */
    private Double totalAmount;
    /** 独立用户数 */
    private Long uniqueUserCount;
    /** 窗口开始时间 */
    private Long windowStart;
    /** 窗口结束时间 */
    private Long windowEnd;
}
