package com.zhisheng.alert.model;

import lombok.Data;

import java.util.List;

/**
 * @
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
public class AtMobiles {
    /**
     * 被@人的手机号
     *
     * @return
     */
    public List<String> atMobiles;

    /**
     * @所有人时:true,否则为:false
     */
    public Boolean isAtAll;
}
