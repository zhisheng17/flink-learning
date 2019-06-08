package com.zhisheng.connectors.akka.utils;

import akka.actor.ActorRef;

import java.io.Serializable;

/**
 * Desc:
 * Created by zhisheng on 2019-06-08
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class SubscribeReceiver implements Serializable {
    private static final long serialVersionUID = 1L;
    private ActorRef receiverActor;

    public SubscribeReceiver(ActorRef receiverActor) {
        this.receiverActor = receiverActor;
    }

    public void setReceiverActor(ActorRef receiverActor) {
        this.receiverActor = receiverActor;
    }

    public ActorRef getReceiverActor() {
        return receiverActor;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SubscribeReceiver) {
            SubscribeReceiver other = (SubscribeReceiver) obj;
            return other.canEquals(this) && super.equals(other)
                    && receiverActor.equals(other.getReceiverActor());
        } else {
            return false;
        }
    }

    public boolean canEquals(Object obj) {
        return obj instanceof SubscribeReceiver;
    }
}
