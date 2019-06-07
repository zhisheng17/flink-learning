package com.zhisheng.connectors.rocketmq;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class RunningChecker implements Serializable {
    private volatile boolean isRunning = false;

    public boolean isRunning() {
        return isRunning;
    }

    public void setRunning(boolean running) {
        isRunning = running;
    }
}
