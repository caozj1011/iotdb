package org.apache.iotdb.write;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class IoTDBInsertManager {
  private static final Logger logger = LoggerFactory.getLogger(IoTDBInsertManager.class);

  public static void main(String[] args) throws IoTDBConnectionException {
    long startAll = System.currentTimeMillis();
    String directoryPath = "/Users/caozhijia/Downloads/Fan1_YP-1";
    FileParser fileParser = new FileParser();
    List<File> fileList = fileParser.getAllFilesInDirectory(directoryPath);
    Map<String, List<File>> partitionList =
        fileList.stream().collect(Collectors.groupingBy(file -> file.getName().substring(3, 5)));

    Set<String> keySet = partitionList.keySet();
    //    CountDownLatch stepOneLatch = new CountDownLatch(3);
    //    for (String partition : keySet) {
    //      AliThreadPoolUtil.execute(new IoTDBInsertTask(partitionList.get(partition),partition,
    // TSEncoding.RLE, CompressionType.GZIP,"root.db1_rle_gzip.p_",stepOneLatch));
    //    }
    //
    //    try {
    //      stepOneLatch.await();
    //    } catch (InterruptedException e) {
    //      logger.error("stepOneLatch await error,",e);
    //    }

    CountDownLatch countDownLatch = new CountDownLatch(3);
    for (String partition : keySet) {
      //      AliThreadPoolUtil.execute(
      //          new IoTDBInsertTask(
      //              partitionList.get(partition),
      //              partition,
      //              TSEncoding.RLE,
      //              CompressionType.ZSTD,
      //              "root.db3_rle_zstd.p_",
      //              countDownLatch));
      AliThreadPoolUtil.execute(
          new IoTDBInsertTask(
              partitionList.get(partition),
              partition,
              TSEncoding.GORILLA,
              CompressionType.ZSTD,
              "root.db1_gorilla_zstd.p_",
              countDownLatch));
    }
    try {
      countDownLatch.await();
    } catch (InterruptedException e) {
      logger.error("countDownLatch await error,", e);
    }
    long endAll = System.currentTimeMillis();
    logger.info("all execute done,cost:{} s.", (endAll - startAll) / 1000);
  }

  static class NumericFileNameComparator implements Comparator<File> {
    @Override
    public int compare(File file1, File file2) {
      // 从文件名中提取数字部分进行比较
      long num1 = extractNumber(file1.getName());
      long num2 = extractNumber(file2.getName());
      return Long.compare(num1, num2);
    }

    private long extractNumber(String fileName) {

      return Long.parseLong(fileName.substring(8, fileName.lastIndexOf(".")).replace("_", ""));
    }
  }
}
