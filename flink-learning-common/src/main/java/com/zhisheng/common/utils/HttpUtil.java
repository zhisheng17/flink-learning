package com.zhisheng.common.utils;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * HTTP 工具类
 *
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class HttpUtil {

    //构造 Basic Auth 的用户名和密码
    private static final String USER_NAME = "";
    private static final String PASSWORD = "";

    private static final CloseableHttpClient httpClient = HttpClients.createDefault();

    public static String doPostString(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        String httpStr;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
            //如果要设置 Basic Auth 的话
//        httpPost.setHeader("Authorization", getHeader());
            response = httpClient.execute(httpPost);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpStr;
    }


    /**
     * 通过GET方式发起http请求
     */
    public static String doGet(String url) {
        HttpGet get = new HttpGet(url);
        get.setHeader("content-type", "application/json");
        //如果要设置 Basic Auth 的话
//        get.setHeader("Authorization", getHeader());
        CloseableHttpResponse httpResponse = null;
        try {
            httpResponse = httpClient.execute(get);
            if (httpResponse.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                HttpEntity entity = httpResponse.getEntity();
                if (null != entity) {
                    return EntityUtils.toString(httpResponse.getEntity());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (httpResponse != null)
                    httpResponse.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }


    public static String doPutString(String url, String jsonParams) throws Exception {
        CloseableHttpResponse response = null;
        HttpPut httpPut = new HttpPut(url);

        String httpStr;
        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPut.setEntity(entity);
            httpPut.setHeader("content-type", "application/json");
            //如果要设置 Basic Auth 的话
//        httpPut.setHeader("Authorization", getHeader());
            response = httpClient.execute(httpPut);
            httpStr = EntityUtils.toString(response.getEntity(), "UTF-8");

        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
                response.close();
            }
        }
        return httpStr;
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
        CloseableHttpResponse response = null;
        HttpPost httpPost = new HttpPost(url);

        try {
            StringEntity entity = new StringEntity(jsonParams, "UTF-8");
            entity.setContentEncoding("UTF-8");
            entity.setContentType("application/json");

            httpPost.setEntity(entity);
            httpPost.setHeader("content-type", "application/json");
            //如果要设置 Basic Auth 的话
//        httpPost.setHeader("Authorization", getHeader());
            response = httpClient.execute(httpPost);
        } finally {
            if (response != null) {
                EntityUtils.consume(response.getEntity());
            }
        }
        return response;
    }


    /**
     * 构造Basic Auth认证头信息
     *
     * @return
     */
    private static String getHeader() {
        String auth = USER_NAME + ":" + PASSWORD;
        byte[] encodedAuth = Base64.encodeBase64(auth.getBytes(Charset.forName("US-ASCII")));
        String authHeader = "Basic " + new String(encodedAuth);
        return authHeader;
    }
}