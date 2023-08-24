package org.apache.iotdb.compaction;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.FastCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.performer.impl.ReadChunkCompactionPerformer;
import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.task.InnerSpaceCompactionTask;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

public class ExecuteCompactionTaskTool {

    public static TsFileManager tsFileManager = new TsFileManager("root.sg","1","");
    public static void main(String[] args) {
        InnerSpaceCompactionTask task =
                new InnerSpaceCompactionTask(
                        0,
                        tsFileManager,
                        Arrays.asList(
                                new TsFileResource(new File("/Users/caozhijia/Desktop/sequence/root.sg/1/2742/1692806875547-1-4-2.tsfile"))
                                ,new TsFileResource(new File("/Users/caozhijia/Desktop/sequence/root.sg/1/2742/1692808959164-22-2-1.tsfile"))
                                ,new TsFileResource(new File("/Users/caozhijia/Desktop/sequence/root.sg/1/2742/1692809448714-27-0-0.tsfile"))),
                        true,
                        new FastCompactionPerformer(false),
                        new AtomicInteger(0),
                        0);
        task.start();

    }
}
