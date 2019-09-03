package com.zhisheng.collector;


import com.zhisheng.common.utils.HttpUtil;
import com.zhisheng.common.utils.PropertiesUtil;

public class FlinkJobMetricCollect {

    public static void main(String[] args) {
        String jobManagerHost = PropertiesUtil.defaultProp.get("flink.jobmanager.host").toString();
        String jobOverviewResult = HttpUtil.doGet("http://" + jobManagerHost + "/jobs/overview");



    }
}
