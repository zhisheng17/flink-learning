package com.zhisheng.alert.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 链接类型钉钉消息
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class LinkMessage extends BaseMessage {

    public Link link;

    @Override
    protected void init() {
        this.msgtype = MessageType.link;
    }

    @Data
    public static class Link {
        /**
         * 消息简介
         */
        private String text;

        /**
         * 消息标题
         */
        private String title;

        /**
         * 封面图片URL
         */
        private String picUrl;

        /**
         * 消息跳转URL
         */
        private String messageUrl;
    }
}
