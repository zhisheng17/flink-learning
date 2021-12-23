package com.zhisheng.libraries.cep.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Desc:
 * Created by zhisheng on 2019/10/29 上午10:34
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Alert {
    private String message;
}
