package com.zhisheng.connectors.rocketmq.common.selector;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class DefaultTopicSelector<T> implements TopicSelector<T> {

    private final String topicName;
    private final String tagName;

    public DefaultTopicSelector(final String topicName) {
        this(topicName, "");
    }

    public DefaultTopicSelector(String topicName, String tagName) {
        this.topicName = topicName;
        this.tagName = tagName;
    }

    @Override
    public String getTopic(T tuple) {
        return topicName;
    }

    @Override
    public String getTag(T tuple) {
        return tagName;
    }
}
