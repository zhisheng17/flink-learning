package com.zhisheng.alert.model;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 钉钉工作通知
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@NoArgsConstructor
public class WorkNotify {

    /**
     * 微应用 id
     */
    private Integer agent_id;

    /**
     * 接收人列表，逗号分隔
     */
    private String userid_list;

    /**
     * 部门 id 列表，可选
     */
    private String dept_id_list;

    /**
     * 是否发送给所有
     */
    private Boolean to_all_user;

    /**
     * 消息体
     */
    private Msg msg;

    @Data
    @AllArgsConstructor
    public static class Msg {

        /**
         * 消息类型
         */
        private String msgtype;

        private Text text;

        @Data
        @AllArgsConstructor
        public static class Text {
            /**
             * 消息内容
             */
            private String content;
        }
    }
}
