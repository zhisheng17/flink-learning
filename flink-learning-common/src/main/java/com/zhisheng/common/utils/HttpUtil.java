package com.zhisheng.common.utils;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class HttpUtil {
    /**
     * 通过GET方式发起http请求
     */
    public static String doGet(String url) {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        try {
            HttpGet get = new HttpGet(url);
//            get.setHeader("Internal-Client", "alert");
            get.setHeader("content-type", "application/json");
            CloseableHttpResponse httpResponse = null;
            httpResponse = httpClient.execute(get);
            try {
                if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    HttpEntity entity = httpResponse.getEntity();
                    if (null != entity) {
                        return EntityUtils.toString(httpResponse.getEntity());
                    }
                }
            } finally {
                httpResponse.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpClient != null) {
                    httpClient.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    /**
     * 发送 POST 请求（HTTP），JSON形式
     *
     * @param url        调用的地址
     * @param jsonParams 调用的参数
     * @return
     * @throws Exception
     */
    public static CloseableHttpResponse doPostResponse(String url, String jsonParams) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
            response = httpClient.execute(httpPost);
        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return response;
    }


    public static String doPostString(String url, String jsonParams) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        String httpStr;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
//            httpPost.setHeader("Internal-Client", "alert");
            response = httpClient.execute(httpPost);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return httpStr;
    }

}
