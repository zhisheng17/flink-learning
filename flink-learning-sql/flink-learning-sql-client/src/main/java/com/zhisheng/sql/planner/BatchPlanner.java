package com.zhisheng.sql.planner;

import com.zhisheng.sql.cli.CliOptions;
import com.zhisheng.sql.cli.SqlCommandParser;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class BatchPlanner extends Planner {

    public static BatchPlanner build(CliOptions options) {
        return new BatchPlanner(options);
    }

    private BatchPlanner(CliOptions options) {
        this.sqlFilePath = options.getSqlFilePath();
        this.workSpace = options.getWorkingSpace();
        this.isTest = options.getIsTest();
        this.k8sClusterId = options.getK8sClusterId();
    }

    @Override
    public void run() throws IOException, ExecutionException, InterruptedException {
        EnvironmentSettings bbSettings = EnvironmentSettings.newInstance().inBatchMode().build();
        this.tEnv = TableEnvironment.create(bbSettings);
        this.statementSet = tEnv.createStatementSet();
        List<String> sql = Files.readAllLines(Paths.get(workSpace + "/" + sqlFilePath));
        List<SqlCommandParser.SqlCommandCall> calls = SqlCommandParser.parse(sql);
        for (SqlCommandParser.SqlCommandCall call : calls) {
            callCommand(call);
        }
        if (!isExplain) {
            TableResult execute = statementSet.execute();
            if (k8sClusterId != null) {
                getK8sStatus(execute);
            } else {
                execute.await();
            }
        }
        LOG.info("Job submitted SUCCESS");
    }

}
