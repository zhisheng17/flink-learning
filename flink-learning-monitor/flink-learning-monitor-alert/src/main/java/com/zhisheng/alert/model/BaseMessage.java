package com.zhisheng.alert.model;

import java.io.Serializable;

/**
 * 请求消息的抽象类
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public abstract class BaseMessage implements Serializable {

    public BaseMessage() {
        init();
    }

    /**
     * 消息类型
     */
    protected MessageType msgtype;


    public MessageType getMsgtype() {
        return msgtype;
    }

    /**
     * 初始化 MessageType 方法
     */
    protected abstract void init();
}
