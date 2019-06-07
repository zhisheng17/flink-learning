package com.zhisheng.connectors.rocketmq.common.selector;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public interface TopicSelector<T> extends Serializable {

    String getTopic(T tuple);

    String getTag(T tuple);

}