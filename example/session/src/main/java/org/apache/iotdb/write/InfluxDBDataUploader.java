package org.apache.iotdb.write;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.iotdb.write.InsertToInfluxdbTask.insertToInfluxdb;

public class InfluxDBDataUploader {

  public static long count = 0;
  public static HashSet hashSet = new HashSet();

  public static void main(String[] args) {
    String directoryPath = "/Users/caozhijia/Downloads/Fan1_YP";
    FileParser fileParser = new FileParser();
    List<File> fileList = fileParser.getAllFilesInDirectory(directoryPath);
    Map<String, List<File>> partitionList =
        fileList.stream().collect(Collectors.groupingBy(file -> file.getName().substring(3, 5)));

    Set<String> keySet = partitionList.keySet();
    for (String partition : keySet) {
      List<File> files = partitionList.get(partition);
      Collections.sort(files, new NumericFileNameComparator());
      for (File file : files) {
        System.out.println("start deal :" + file.getName());
        long start = System.currentTimeMillis();
        try {
          List<String[]> linesList = FileParser.parseFileLines(file);
          insertToInfluxdb(linesList, partition);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();
        System.out.println(file.getName() + ": done,cost:" + (end - start) / 1000 + "s");
      }
    }
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

  public static long convertToUnixTimestampMillis(String dateString) {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'T'yyyy-MM-dd HH:mm:ss.SSSSSS");

    // 解析字符串为 LocalDateTime
    LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);

    // 转换为毫秒精度的时间戳
    return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }
}
