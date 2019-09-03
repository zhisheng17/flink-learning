package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc: 单个店铺的订单
 * Created by zhisheng on 2019-04-18
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderEvent {

    /**
     * Order Id
     */
    private Long id;

    /**
     * 购物单 Id
     */
    private Long purchaseOrderId;

    /**
     * Order device source（1：IOS、2：PC、3：Android）
     */
    private int deviceSource;

    /**
     * 买家 Id
     */
    private Long buyerId;

    /**
     * 买家 name
     */
    private String buyerName;

    /**
     * 店铺 Id
     */
    private String shopId;

    /**
     * 店铺 name
     */
    private String shopName;

    /**
     * 支付状态（0：未支付、1：已支付）
     */
    private int payStatus;

    /**
     * 支付完成时间（timestamp）
     */
    private Long payAt;

    /**
     * 实际支付金额（以分为单位）
     */
    private Long payAmount;

    /**
     * 发货状态（0：未发货、1：待发货、2：已发货）
     */
    private int deliveryStatus;

    /**
     * 签收状态（0：未签收、1：已签收）
     */
    private int receiveStatus;

    /**
     * 退货状态（0：未退货、1：已退货）
     */
    private int reverseStatus;

    /**
     * 发货时间（timestamp）
     */
    private Long shippingAt;

    /**
     * 确认收货时间（timestamp）
     */
    private Long confirmAt;

    /**
     * 买家留言
     */
    private String buyerNotes;

}
