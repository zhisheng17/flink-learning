package com.zhisheng.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Desc: 商品
 * Created by zhisheng on 2019-04-18
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class ProductEvent {

    /**
     * Product Id
     */
    private Long id;

    /**
     * Product 类目 Id
     */
    private Long categoryId;

    /**
     * Product 编码
     */
    private String code;

    /**
     * Product 店铺 Id
     */
    private Long shopId;

    /**
     * Product 店铺 name
     */
    private String shopName;

    /**
     * Product 品牌 Id
     */
    private Long brandId;

    /**
     * Product 品牌 name
     */
    private String brandName;

    /**
     * Product name
     */
    private String name;

    /**
     * Product 图片地址
     */
    private String imageUrl;

    /**
     * Product 状态（1(上架),-1(下架),-2(冻结),-3(删除)）
     */
    private int status;

    /**
     * Product 类型
     */
    private int type;

    /**
     * Product 标签
     */
    private List<String> tags;

    /**
     * Product 价格（以分为单位）
     */
    private Long price;
}
