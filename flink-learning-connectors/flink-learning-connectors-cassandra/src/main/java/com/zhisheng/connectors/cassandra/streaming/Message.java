package com.zhisheng.connectors.cassandra.streaming;

import com.datastax.driver.mapping.annotations.Column;
import com.datastax.driver.mapping.annotations.Table;
import lombok.AllArgsConstructor;

/**
 * Desc:
 * Created by zhisheng on 2019-08-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Table(keyspace = "test", name = "message")
@AllArgsConstructor
public class Message {
    private static final long serialVersionUID = 1123119384361005680L;

    @Column(name = "body")
    private String message;

    public Message() {
        this(null);
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String word) {
        this.message = word;
    }

    public boolean equals(Object other) {
        if (other instanceof Message) {
            Message that = (Message) other;
            return this.message.equals(that.message);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return message.hashCode();
    }
}
