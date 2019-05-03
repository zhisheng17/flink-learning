package com.zhisheng.connectors.flume.utils;

import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientConfigurationConstants;
import org.apache.flume.api.RpcClientFactory;

import java.util.Properties;

/**
 * Desc:
 * Created by zhisheng on 2019-05-04
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class FlumeUtil {
    private static final String CLIENT_TYPE_KEY = "client.type";
    private static final String CLIENT_TYPE_DEFAULT_FAILOVER = "default_failover";
    private static final String CLIENT_TYPE_DEFAULT_LOADBALANCING = "default_loadbalance";

    public static RpcClient getRpcClient(String clientType, String hostname, Integer port, Integer batchSize) {
        Properties props;
        RpcClient client;
        switch(clientType.toUpperCase()) {
            case "THRIFT":
                client = RpcClientFactory.getThriftInstance(hostname, port, batchSize);
                break;
            case "DEFAULT":
                client = RpcClientFactory.getDefaultInstance(hostname, port, batchSize);
                break;
            case "DEFAULT_FAILOVER":
                props = getDefaultProperties(hostname, port, batchSize);
                props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_FAILOVER);
                client = RpcClientFactory.getInstance(props);
                break;
            case "DEFAULT_LOADBALANCE":
                props = getDefaultProperties(hostname, port, batchSize);
                props.put(CLIENT_TYPE_KEY, CLIENT_TYPE_DEFAULT_LOADBALANCING);
                client = RpcClientFactory.getInstance(props);
                break;
            default:
                throw new IllegalStateException("Unsupported client type - cannot happen");
        }
        return client;
    }

    public static void destroy(RpcClient client) {
        if (null != client) {
            client.close();
        }
    }

    private static Properties getDefaultProperties(String hostname, Integer port, Integer batchSize) {
        Properties props = new Properties();
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS, "h1");
        props.setProperty(RpcClientConfigurationConstants.CONFIG_HOSTS_PREFIX + "h1",
                hostname + ":" + port.intValue());
        props.setProperty(RpcClientConfigurationConstants.CONFIG_BATCH_SIZE, batchSize.toString());
        return props;
    }
}
