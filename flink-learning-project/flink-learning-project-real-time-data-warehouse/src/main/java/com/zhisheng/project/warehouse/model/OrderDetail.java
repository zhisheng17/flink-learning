package com.zhisheng.project.warehouse.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DWD 层 - 订单明细宽表
 * 将订单主表和商品信息关联后的宽表模型
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderDetail {
    /** 订单 ID */
    private String orderId;
    /** 用户 ID */
    private String userId;
    /** 商品 ID */
    private String productId;
    /** 商品名称 */
    private String productName;
    /** 商品类别 */
    private String category;
    /** 商品单价 */
    private Double price;
    /** 购买数量 */
    private Integer quantity;
    /** 订单金额 */
    private Double totalAmount;
    /** 支付方式：ALIPAY, WECHAT, CREDIT_CARD */
    private String paymentMethod;
    /** 订单状态：CREATED, PAID, SHIPPED, COMPLETED, CANCELLED */
    private String orderStatus;
    /** 用户所在省份 */
    private String province;
    /** 订单创建时间 */
    private Long createTime;
}
