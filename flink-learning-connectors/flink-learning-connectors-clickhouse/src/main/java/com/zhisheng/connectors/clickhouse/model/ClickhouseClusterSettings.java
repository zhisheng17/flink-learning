package com.zhisheng.connectors.clickhouse.model;

import com.google.common.base.Preconditions;
import com.zhisheng.connectors.clickhouse.util.ConfigUtil;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

/**
 * Desc:
 * Created by zhisheng on 2019/9/28 上午12:40
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class ClickhouseClusterSettings {
    public static final String CLICKHOUSE_HOSTS = "clickhouse.access.hosts";
    public static final String CLICKHOUSE_USER = "clickhouse.access.user";
    public static final String CLICKHOUSE_PASSWORD = "clickhouse.access.password";

    private final List<String> hostsWithPorts;
    private final String user;
    private final String password;
    private final String credentials;
    private final boolean authorizationRequired;

    private int currentHostId = 0;

    public ClickhouseClusterSettings(Map<String, String> parameters) {
        Preconditions.checkNotNull(parameters);

        String hostsString = parameters.get(CLICKHOUSE_HOSTS);
        Preconditions.checkNotNull(hostsString);

        hostsWithPorts = buildHostsAndPort(hostsString);
        Preconditions.checkArgument(hostsWithPorts.size() > 0);

        String usr = parameters.get(CLICKHOUSE_USER);
        String pass = parameters.get(CLICKHOUSE_PASSWORD);

        if (StringUtils.isNotEmpty(usr) && StringUtils.isNotEmpty(pass)) {
            user = parameters.get(CLICKHOUSE_USER);
            password = parameters.get(CLICKHOUSE_PASSWORD);

            credentials = buildCredentials(user, password);
            authorizationRequired = true;
        } else {
            // avoid NPE
            credentials = "";
            password = "";
            user = "";
            authorizationRequired = false;
        }
    }

    private static List<String> buildHostsAndPort(String hostsString) {
        return Arrays.stream(hostsString
                .split(ConfigUtil.HOST_DELIMITER))
                .map(ClickhouseClusterSettings::checkHttpAndAdd)
                .collect(Collectors.toList());
    }

    private static String checkHttpAndAdd(String host) {
        String newHost = host.replace(" ", "");
        if (!newHost.contains("http")) {
            return "http://" + newHost;
        }
        return newHost;
    }

    private static String buildCredentials(String user, String password) {
        Base64.Encoder x = Base64.getEncoder();
        String credentials = String.join(":", user, password);
        return new String(x.encode(credentials.getBytes()));
    }

    public String getRandomHostUrl() {
        currentHostId = ThreadLocalRandom.current().nextInt(hostsWithPorts.size());
        return hostsWithPorts.get(currentHostId);
    }

    public String getNextHost() {
        if (currentHostId >= hostsWithPorts.size() - 1) {
            currentHostId = 0;
        } else {
            currentHostId += 1;
        }
        return hostsWithPorts.get(currentHostId);
    }

    public List<String> getHostsWithPorts() {
        return hostsWithPorts;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getCredentials() {
        return credentials;
    }

    public boolean isAuthorizationRequired() {
        return authorizationRequired;
    }

    @Override
    public String toString() {
        return "ClickhouseClusterSettings{" +
                "hostsWithPorts=" + hostsWithPorts +
                ", credentials='" + credentials + '\'' +
                ", authorizationRequired=" + authorizationRequired +
                ", currentHostId=" + currentHostId +
                '}';
    }
}
