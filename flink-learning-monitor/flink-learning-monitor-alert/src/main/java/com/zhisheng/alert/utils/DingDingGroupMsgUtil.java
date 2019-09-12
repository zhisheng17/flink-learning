package com.zhisheng.alert.utils;

import com.zhisheng.alert.model.AtMobiles;
import com.zhisheng.alert.model.LinkMessage;
import com.zhisheng.alert.model.MarkDownMessage;
import com.zhisheng.alert.model.TextMessage;
import com.zhisheng.common.utils.GsonUtil;
import com.zhisheng.common.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * 钉钉群消息工具类
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class DingDingGroupMsgUtil {
    /**
     * 组装发送的文本信息
     *
     * @param isAtAll    是否需要 @所有人
     * @param msgContent 要发送信息的主体
     * @param telList    要 @人的电话号码,如果@单独的几个人，就传一个空list，而不是 null
     * @return
     */
    public static String setTextMessage(boolean isAtAll, String msgContent, List<String> telList) {

        AtMobiles atMobiles = new AtMobiles();
        atMobiles.setIsAtAll(isAtAll);
        atMobiles.setAtMobiles(telList);

        TextMessage.TextContent content = new TextMessage.TextContent();
        content.setContent(msgContent);

        return GsonUtil.toJson(new TextMessage(content, atMobiles));
    }

    /**
     * 组装发送的 markdown 信息
     *
     * @param isAtAll 是否需要 @所有人
     * @param title   要发送信息的标题
     * @param text    要发送信息的主体
     * @param telList 要 @人的电话号码,如果@单独的几个人，就传一个空list，而不是 null
     * @return
     */
    public static String setMarkdownMessage(boolean isAtAll, String title, String text, List<String> telList) {

        AtMobiles atMobiles = new AtMobiles();
        atMobiles.setIsAtAll(isAtAll);
        atMobiles.setAtMobiles(telList);

        MarkDownMessage.MarkDownContent content = new MarkDownMessage.MarkDownContent();
        content.setTitle(title);
        content.setText(text);

        return GsonUtil.toJson(new MarkDownMessage(content, atMobiles));
    }

    /**
     * 组装发送的 link 信息
     *
     * @param messageUrl 消息跳转 url
     * @param picUrl     图片 url
     * @param text       要发送信息的主体
     * @param title      要发送信息的标题
     * @return
     */
    public static String setLinkMessage(String messageUrl, String picUrl, String text, String title) {

        LinkMessage.Link link = new LinkMessage.Link();
        link.setTitle(title);
        link.setText(text);
        link.setPicUrl(picUrl);
        link.setMessageUrl(messageUrl);

        return GsonUtil.toJson(new LinkMessage(link));
    }


    /**
     * 给具体的钉钉机器人发送消息
     *
     * @param url 钉钉机器人地址
     * @param msg 消息内容
     * @return 发送消息返回的结果
     */
    public static String sendDingDingMsg(String url, String msg) {
        String result = null;
        try {
            result = HttpUtil.doPostString(url, msg);
        } catch (Exception e) {
            log.error("send ding ding msg has an error, detail : {}", e);
        }
        return result;
    }


    /**
     * 同时给多个钉钉机器人发送消息
     *
     * @param urls 多个钉钉机器人地址
     * @param msg 消息内容
     */
    public static void sendDingDingMsg(List<String> urls, String msg) {
        if (urls == null || urls.size() <= 0) {
            log.warn("you may forget add notify dingding hook");
            return;
        }
        for (String url : urls) {
            sendDingDingMsg(url, msg);
        }
    }


    /**
     * 给一个钉钉机器人同时发送多条消息（注意：钉钉本身的限制 1分钟内一个机器人最多发送 20 条消息）
     *
     * @param url 钉钉地址
     * @param msgs 多条消息
     */
    public static void sendDingDingMsg(String url, List<String> msgs) {
        if (msgs == null || msgs.size() <= 0) {
            log.warn("you may forget add notify msg");
            return;
        }
        for (String msg: msgs) {
            sendDingDingMsg(url, msg);
        }
    }


    /**
     * 给多个钉钉群发送多条消息
     *
     * @param urls 多个钉钉地址
     * @param msgs 多条消息
     */
    public static void sendDingDingMsg(List<String> urls, List<String> msgs) {
        if (msgs == null || msgs.size() <= 0) {
            log.warn("you may forget add notify msg");
            return;
        }
        if (urls == null || urls.size() <= 0) {
            log.warn("you may forget add notify dingding hook");
            return;
        }
        for (String url : urls) {
            for (String msg: msgs) {
                sendDingDingMsg(url, msg);
            }
        }
    }
}
