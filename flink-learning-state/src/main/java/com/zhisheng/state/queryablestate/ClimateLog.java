package com.zhisheng.state.queryablestate;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * Desc:
 * Created by zhisheng on 2019-07-05
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Data
@AllArgsConstructor
public class ClimateLog {
   private String country;
   private String state;
   private float temperature;
   private float humidity;
}
