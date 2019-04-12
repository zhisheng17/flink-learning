package com.zhisheng.alert.utils;


import com.google.common.base.Throwables;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.jayway.jsonpath.JsonPath;
import com.zhisheng.common.utils.HttpUtil;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;


/**
 * 获取access_token
 * https://open-doc.dingtalk.com/microapp/serverapi2/eev437
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
@Slf4j
public class DingDingAccessTokenUtil {

    public static final String DING_DING_ACCESS_TOKEN = "access_token";
    public static final String DING_DING_GET_TOKEN_URL = "https://oapi.dingtalk.com/gettoken";


    public static Cache<String, String> expireCache = CacheBuilder.newBuilder()
            .expireAfterAccess(2, TimeUnit.HOURS)
            .build();

    /**
     * 根据微应用 key 和 secret 获取 access_token
     *
     * @param appKey
     * @param appSecret
     * @return
     */
    public static String getAccessToken(String appKey, String appSecret) {
        String accessToken = "";
        String dingDingAccessTocken = expireCache.getIfPresent(DING_DING_ACCESS_TOKEN);
        if (dingDingAccessTocken == null || "".equals(dingDingAccessTocken)) {
            String result = "";
            try {
                result = HttpUtil.doGet(DING_DING_GET_TOKEN_URL + "?appkey=" + appKey + "&appsecret=" + appSecret);
                accessToken = JsonPath.read(result, "$.access_token");
                if (accessToken != null && !"".equals(accessToken)) {
                    expireCache.put(DING_DING_ACCESS_TOKEN, String.valueOf(accessToken));
                    log.info("get ding ding access token = {}", String.valueOf(accessToken));
                }
            } catch (Exception e) {
                log.error("--------------httpclient do get request exception , msg = {}, result = {}",
                        Throwables.getStackTraceAsString(e), result);
            }
        } else {
            accessToken = dingDingAccessTocken;
        }
        return accessToken;
    }
}
