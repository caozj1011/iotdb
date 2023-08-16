package org.apache.iotdb.write;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static org.apache.iotdb.write.InfluxDBDataUploader.convertToUnixTimestampMillis;

public class InsertToInfluxdbTask {
  public static final String[] influxDBColumns = {
    "", "", "s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8", "s9", "s10", "s11", "s12", "s13", "s14",
    "s15", "s16", "s17", "s18", "s19", "s20", "s21"
  };
  public static final String influxDBUrl = "http://172.20.31.38:8086/write?db=test4&precision=ms";
  public static HashSet<Long> hashSet = new HashSet<>();
  public static long count = 0;

  public InsertToInfluxdbTask() {}

  public static void insertToInfluxdb(List<String[]> values, String partition) {
    try {
      List<String> requests = generateInfluxDBRequests(influxDBColumns, values, partition);
      sendInfluxDBRequests(requests, influxDBUrl);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static List<String> generateInfluxDBRequests(
      String[] columns, List<String[]> data, String partition) {
    List<String> requests = new ArrayList<>();
    for (String[] parts : data) {
      long time = convertToUnixTimestampMillis(parts[0] + " " + parts[1]);
      hashSet.add(time);
      StringBuilder request = new StringBuilder();
      for (int i = 2; i < columns.length; i++) {
        request
            .append(partition)
            .append("_")
            .append(columns[i])
            .append(" value")
            .append("=")
            .append(parts[i])
            .append(" ")
            .append(time)
            .append(System.getProperty("line.separator"));
      }
      requests.add(request.toString());
    }
    return requests;
  }

  public static void sendInfluxDBRequests(List<String> requests, String influxDBUrl)
      throws IOException {
    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      for (String request : requests) {
        HttpPost httpPost = new HttpPost(influxDBUrl);
        StringEntity requestEntity = new StringEntity(request);
        httpPost.setEntity(requestEntity);
        HttpResponse response = httpClient.execute(httpPost);
        count++;
        if (count % 10000 == 0) {
          System.out.println("count is :" + count);
          System.out.println("set is :" + hashSet.size());
        }
      }
    }
  }
}
