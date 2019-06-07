package com.zhisheng.connectors.rocketmq.common.selector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SimpleTopicSelector implements TopicSelector<Map> {

    private static final Logger LOG = LoggerFactory.getLogger(SimpleTopicSelector.class);

    private final String topicFieldName;
    private final String defaultTopicName;

    private final String tagFieldName;
    private final String defaultTagName;


    public SimpleTopicSelector(String topicFieldName, String defaultTopicName, String tagFieldName, String defaultTagName) {
        this.topicFieldName = topicFieldName;
        this.defaultTopicName = defaultTopicName;
        this.tagFieldName = tagFieldName;
        this.defaultTagName = defaultTagName;
    }

    @Override
    public String getTopic(Map tuple) {
        if (tuple.containsKey(topicFieldName)) {
            Object topic = tuple.get(topicFieldName);
            return topic != null ? topic.toString() : defaultTopicName;
        } else {
            LOG.warn("Field {} Not Found. Returning default topic {}", topicFieldName, defaultTopicName);
            return defaultTopicName;
        }
    }

    @Override
    public String getTag(Map tuple) {
        if (tuple.containsKey(tagFieldName)) {
            Object tag = tuple.get(tagFieldName);
            return tag != null ? tag.toString() : defaultTagName;
        } else {
            LOG.warn("Field {} Not Found. Returning default tag {}", tagFieldName, defaultTagName);
            return defaultTagName;
        }
    }
}
