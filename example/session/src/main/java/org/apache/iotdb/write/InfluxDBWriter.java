package org.apache.iotdb.write;

import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.write.InfluxDBDataUploader.convertToUnixTimestampMillis;

public class InfluxDBWriter {
  public static final String[] influxDBColumns = {
    "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14", "s15",
    "s16", "s17", "s18", "s19", "s20", "s21", "s22"
  };
  private static final String DATABASE_NAME = "test7";
  private static final String USERNAME = "your_username";
  private static final String PASSWORD = "your_password";
  private static final String INFLUXDB_URL = "http://172.20.31.38:8086";
  static InfluxDBWriter writer = new InfluxDBWriter();

  private static InfluxDB influxDB;

  private static HashSet<Long> hashSet = new HashSet<>();

  private static long count = 0;

  public InfluxDBWriter() {
    influxDB = InfluxDBFactory.connect(INFLUXDB_URL);
  }

  public void writeData(String measurement, String field, Double value, long timestamp) {}

  public QueryResult executeQuery(String query) {
    Query queryObject = new Query(query, DATABASE_NAME);
    return influxDB.query(queryObject);
  }

  public void close() {
    influxDB.close();
  }

  public static void main(String[] args) {
    long startAll = System.currentTimeMillis();
    String directoryPath = "/Users/caozhijia/Downloads/Fan1_YP/p1";
    FileParser fileParser = new FileParser();
    List<File> fileList = fileParser.getAllFilesInDirectory(directoryPath);
    Map<String, List<File>> partitionList =
        fileList.stream().collect(Collectors.groupingBy(file -> file.getName().substring(3, 5)));

    Set<String> keySet = partitionList.keySet();
    for (String partition : keySet) {
      List<File> files = partitionList.get(partition);
      Collections.sort(files, new InfluxDBDataUploader.NumericFileNameComparator());

      for (File file : files) {
        System.out.println("start deal :" + file.getName());
        long start = System.currentTimeMillis();
        try {
          List<String[]> linesList = FileParser.parseFileLines(file);
          BatchPoints batchPoints =
              BatchPoints.database(DATABASE_NAME)
                  .retentionPolicy("autogen")
                  .consistency(InfluxDB.ConsistencyLevel.ALL)
                  .build();
          for (String[] values : linesList) {
            long time = convertToUnixTimestampMillis(values[0] + " " + values[1]);
            hashSet.add(time);
            for (int i = 2; i < values.length; i++) {
              Point point =
                  Point.measurement("p_" + partition + influxDBColumns[i - 2])
                      .addField("value", Double.parseDouble(values[i]))
                      .time(time, TimeUnit.MILLISECONDS)
                      .build();

              batchPoints.point(point);
            }
            count++;

            if (batchPoints.getPoints().size() == 6600) {
              try {
                influxDB.write(batchPoints);
              } catch (Exception e) {

              }
              batchPoints =
                  BatchPoints.database(DATABASE_NAME)
                      .retentionPolicy("autogen")
                      .consistency(InfluxDB.ConsistencyLevel.ALL)
                      .build();
            }
          }
          if (!batchPoints.getPoints().isEmpty()) {
            try {
              influxDB.write(batchPoints);
            } catch (Exception e) {

            }
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        long end = System.currentTimeMillis();

        System.out.println(
            file.getName() + ": done,cost:" + (end - start) / 1000 + "s,count:" + count);
      }
    }
    long endAll = System.currentTimeMillis();
    System.out.println("all done,cost:" + (endAll - startAll) / 1000 + "s,count:" + count);

    writer.close();
  }
}
