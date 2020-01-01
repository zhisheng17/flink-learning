package com.zhisheng.state.metadata;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.savepoint.Savepoint;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;

import java.io.*;
import java.util.Map;

/**
 * @author fanrui
 * @time 2019-12-08 17:06:10
 */
public class MetadataSerializer {


    public static void main(String[] args) throws IOException {
        //  读取元数据文件
        File f = new File("flink-learning-state/src/main/resources/_metadata");
        // 第二步，建立管道,FileInputStream文件输入流类用于读文件
        FileInputStream fis = new FileInputStream(f);
        BufferedInputStream bis = new BufferedInputStream(fis);
        DataInputStream dis = new DataInputStream(bis);

        // 通过 Flink 的 Checkpoints 类解析元数据文件
        Savepoint savepoint = Checkpoints.loadCheckpointMetadata(dis,
                MetadataSerializer.class.getClassLoader());
        // 打印当前的 CheckpointId
        System.out.println(savepoint.getCheckpointId());

        // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
        // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
        for (OperatorState operatorState : savepoint.getOperatorStates()) {
            System.out.println(operatorState);
            // 当前算子的状态大小为 0 ，表示算子不带状态，直接退出
            if (operatorState.getStateSize() == 0) {
                continue;
            }

            // 遍历当前算子的所有 subtask
            for (OperatorSubtaskState operatorSubtaskState : operatorState.getStates()) {
                // 解析 operatorSubtaskState 的 ManagedKeyedState
                parseManagedKeyedState(operatorSubtaskState);
                // 解析 operatorSubtaskState 的 ManagedOperatorState
                parseManagedOperatorState(operatorSubtaskState);
            }
        }
    }


    /**
     * 解析 operatorSubtaskState 的 ManagedKeyedState
     *
     * @param operatorSubtaskState operatorSubtaskState
     */
    private static void parseManagedKeyedState(OperatorSubtaskState operatorSubtaskState) {
        // 遍历当前 subtask 的 KeyedState
        for (KeyedStateHandle keyedStateHandle : operatorSubtaskState.getManagedKeyedState()) {
            // 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
            // 因此仅处理 IncrementalRemoteKeyedStateHandle
            if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
                // 获取 RocksDB 的 sharedState
                Map<StateHandleID, StreamStateHandle> sharedState =
                        ((IncrementalRemoteKeyedStateHandle) keyedStateHandle).getSharedState();
                // 遍历 sharedState 中所有的 sst 文件，key 为 sst 文件名，value 为对应的 hdfs 文件 Handle
                for (Map.Entry<StateHandleID, StreamStateHandle> entry : sharedState.entrySet()) {
                    // 打印 sst 文件名
                    System.out.println("sstable 文件名：" + entry.getKey());
                    if (entry.getValue() instanceof FileStateHandle) {
                        Path filePath = ((FileStateHandle) entry.getValue()).getFilePath();
                        // 打印 sst 文件对应的 hdfs 文件位置
                        System.out.println("sstable 文件对应的 hdfs 位置：" + filePath.getPath());
                    }
                }
            }
        }
    }


    /**
     * 解析 operatorSubtaskState 的 ManagedOperatorState
     *
     * @param operatorSubtaskState operatorSubtaskState
     */
    private static void parseManagedOperatorState(OperatorSubtaskState operatorSubtaskState) {
        // 遍历当前 subtask 的 OperatorState
        for (OperatorStateHandle operatorStateHandle : operatorSubtaskState.getManagedOperatorState()) {
            StreamStateHandle delegateStateHandle = operatorStateHandle.getDelegateStateHandle();
            if (delegateStateHandle instanceof FileStateHandle) {
                Path filePath = ((FileStateHandle) delegateStateHandle).getFilePath();
                System.out.println(filePath.getPath());
            }
        }
    }

}
