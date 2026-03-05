package com.zhisheng.project.common.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 页面访问事件模型
 * 用于实时大屏展示 PV/UV 统计
 *
 * @author zhisheng
 */
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class PageAccessEvent {
    /** 事件 ID */
    private String eventId;
    /** 用户 ID */
    private String userId;
    /** 页面 URL */
    private String pageUrl;
    /** 页面标题 */
    private String pageTitle;
    /** 页面类别，如：首页、商品详情页、购物车 */
    private String pageCategory;
    /** 来源渠道：PC, APP, H5 */
    private String channel;
    /** 停留时长（毫秒） */
    private Long stayDuration;
    /** 访问时间戳 */
    private Long timestamp;
    /** 用户设备类型 */
    private String deviceType;
    /** 用户所在地区 */
    private String region;
}
