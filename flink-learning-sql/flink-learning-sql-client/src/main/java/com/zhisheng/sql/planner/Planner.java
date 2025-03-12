package com.zhisheng.sql.planner;

import com.zhisheng.sql.cli.SqlCommandParser;
import com.zhisheng.sql.constant.Constant;
import com.zhisheng.sql.constant.UnitEnum;
import com.zhisheng.sql.utils.CloseableRowIteratorWrapper;
import com.zhisheng.sql.utils.Config;
import com.zhisheng.sql.utils.HttpClient;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.table.api.SqlParserException;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

public abstract class Planner {

    protected static final Logger LOG = LoggerFactory.getLogger(Planner.class);

    protected String sqlFilePath;
    protected String workSpace;
    protected String isTest;
    protected String k8sClusterId;
    protected boolean isExplain;
    protected StatementSet statementSet;
    protected TableEnvironment tEnv;

    public abstract void run() throws IOException, ExecutionException, InterruptedException;

    protected void getK8sStatus(TableResult execute) {
        //todo：注入状态监听器上报任务状态
        CloseableIterator<Row> collect = execute.collect();
        CloseableRowIteratorWrapper data = new CloseableRowIteratorWrapper(collect);
        Optional<JobClient> jobClient = execute.getJobClient();
        String finalJobStatus = null;
        while (true) {
            try {
                if (jobClient.isPresent()) {
                    JobClient jobClientTrue = jobClient.get();
                    JobStatus jobStatus = jobClientTrue.getJobStatus().get();
                    finalJobStatus = jobStatus.name();
                    Thread.sleep(500);
                }
                if (data.isFirstRowReady() && (JobStatus.FAILED.name().equals(finalJobStatus) || JobStatus.FINISHED.name().equals(finalJobStatus))) {
                    String httpUrl = Config.getString("httpUrl");
                    LOG.info("== sql状态监听 ==");
                    LOG.info(String.format("== 请求 %s ==", httpUrl));
                    LOG.info(String.format("== 状态为 %s ==", finalJobStatus));
                    // 调用外部实时平台接口通知状态
                    HttpClient.doGet(httpUrl + "?k8sClusterId=" + k8sClusterId + "&monitorType=1&" + "flinkStatus=" + finalJobStatus);
                    break;
                }
            } catch (Exception e) {
                break;
            }
        }
    }

    protected void callCommand(SqlCommandParser.SqlCommandCall cmdCall) {
        switch (cmdCall.command) {
            case SET:
                callSet(cmdCall);
                break;
            case CREATE_TABLE:
            case CREATE_CATALOG:
            case USER_CATALOG:
            case CREATE_VIEW:
                callUpdate(cmdCall);
                break;
            case EXPLAIN_FOR:
                explain(cmdCall);
                break;
            case INSERT_INTO:
            case INSERT_OVERWRITE:
                callInsert(cmdCall);
                break;
            case CREATE_FUNCTION:
                callCreateFunction(cmdCall);
                break;
            default:
                throw new RuntimeException("Unsupported command: " + cmdCall.command);
        }
    }

    private void callInsert(SqlCommandParser.SqlCommandCall cmdCall) {
        String dml = cmdCall.operands[0];
        statementSet.addInsertSql(dml);
    }

    private void callSet(SqlCommandParser.SqlCommandCall cmdCall) {
        LOG.info("set:" + cmdCall.operands[0] + "=" + cmdCall.operands[1]);
        String key = cmdCall.operands[0];
        String value = cmdCall.operands[1];

        if (Constant.IDLE_STATERETENTIOO_TIME.equals(key)) {
            String unit = value.replaceAll("\\d+", "");
            String number = value.replaceAll("\\w+", "");
            UnitEnum duration = UnitEnum.valueOf(unit);
            switch (duration) {
                case m:
                    tEnv.getConfig().setIdleStateRetention(Duration.ofMinutes(Long.parseLong(number)));
                    break;
                case h:
                    tEnv.getConfig().setIdleStateRetention(Duration.ofHours(Long.parseLong(number)));
                    break;
                case d:
                    tEnv.getConfig().setIdleStateRetention(Duration.ofDays(Long.parseLong(number)));
                    break;
                default:
                    break;
            }

        } else {
            tEnv.getConfig().getConfiguration().setString(key, value);
        }
    }


    private void callUpdate(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void explain(SqlCommandParser.SqlCommandCall cmdCall) {
        String ddl = cmdCall.operands[0];
        try {
            this.isExplain = true;
            tEnv.executeSql(ddl).print();
        } catch (SqlParserException e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }

    private void callCreateFunction(SqlCommandParser.SqlCommandCall cmdCall) {
        LOG.info("create function:" + cmdCall.operands[0]);
        String ddl = cmdCall.operands[0];
        try {
            tEnv.executeSql(ddl);
        } catch (Exception e) {
            throw new RuntimeException("SQL parse failed:\n" + ddl + "\n", e);
        }
    }
}
