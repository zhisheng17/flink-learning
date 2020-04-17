package com.zhisheng.sql.ago.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by zhisheng on 2019-06-02
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WC {
    /**
     * word
     */
    public String word;

    /**
     * 出现的次数
     */
    public long c;
}
