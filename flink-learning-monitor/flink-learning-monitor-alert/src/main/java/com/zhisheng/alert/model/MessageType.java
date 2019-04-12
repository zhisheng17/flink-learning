package com.zhisheng.alert.model;

/**
 * 消息类型：文本、链接、MarkDown、跳转卡片、消息卡片五种枚举值
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public enum MessageType {
    /**
     * 文本类型
     */
    text,

    /**
     * 链接类型
     */
    link,

    /**
     * MarkDown类型
     */
    markdown,

    /**
     * 跳转卡片类型
     */
    actionCard,

    /**
     * 消息卡片类型
     */
    feedCard;
}
