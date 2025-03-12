package com.zhisheng.sql.cli;


public class CliOptions {

    private final String sqlFilePath;
    private final String workingSpace;
    private final String isTest;
    private final String isBatch;
    private final String k8sClusterId;

    public CliOptions(String sqlFilePath, String workingSpace, String isTest, String isBatch, String k8sClusterId) {
        this.sqlFilePath = sqlFilePath;
        this.workingSpace = workingSpace;
        this.isTest = isTest;
        this.isBatch = isBatch;
        this.k8sClusterId = k8sClusterId;
    }

    public String getSqlFilePath() {
        return sqlFilePath;
    }

    public String getWorkingSpace() {
        return workingSpace;
    }

    public String getIsTest() {
        return isTest;
    }

    public String getIsBatch() {
        return isBatch;
    }

    public String getK8sClusterId() {
        return k8sClusterId;
    }
}
