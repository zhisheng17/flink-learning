package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Desc:
 * Created by zhisheng on 2019-04-18
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class OrderLineEvent {

    /**
     * 订单行 Id
     */
    private Long orderLineId;

    /**
     * 购物单 Id
     */
    private Long purchaseOrderId;

    /**
     * 店铺订单 Id
     */
    private Long orderId;

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

    /**
     * 订单来源（1：IOS、2：PC、3：Android）
     */
    private int deviceSource;

    /**
     * 商品 name
     */
    private String name;

    /**
     * 商品 id
     */
    private Long id;

    /**
     * 商品标签
     */
    private List<String> tags;

    /**
     * 购买数量
     */
    private int count;

    /**
     * 实际支付金额
     */
    private Long payAmount;
}
