package org.apache.iotdb.write;

import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class IoTDBInsertTask implements Runnable {

  private static final Logger logger = LoggerFactory.getLogger(IoTDBInsertTask.class);

  private List<File> files;
  private String partition;
  private TSEncoding tsEncoding;
  private CompressionType compressionType;
  private String database;
  private Session session;
  private HashSet<Long> set = new HashSet<>();
  private CountDownLatch countDownLatch;

  public IoTDBInsertTask(
      List<File> files,
      String partition,
      TSEncoding tsEncoding,
      CompressionType compressionType,
      String database,
      CountDownLatch countDownLatch) {
    this.files = files;
    this.partition = partition;
    this.tsEncoding = tsEncoding;
    this.compressionType = compressionType;
    this.database = database;
    this.countDownLatch = countDownLatch;
    this.session =
        new Session.Builder()
            .host("172.20.31.49")
            .port(6667)
            .username("root")
            .password("root")
            .version(Version.V_1_0)
            .build();
    try {
      this.session.open();
    } catch (IoTDBConnectionException e) {
      logger.error("connect error!", e);
    }
  }

  private void insert() throws IoTDBConnectionException {
    files.sort(new IoTDBInsertManager.NumericFileNameComparator());
    long outStart = System.currentTimeMillis();

    List<MeasurementSchema> schemaList = new ArrayList<>();
    for (int i = 1; i <= 22; i++) {
      try {
        session.createTimeseries(
            database + partition + ".s" + i, TSDataType.DOUBLE, tsEncoding, compressionType);
      } catch (StatementExecutionException e) {
        logger.error("create timeseries error,", e);
      }

      schemaList.add(new MeasurementSchema("s" + i, TSDataType.DOUBLE));
    }

    for (File file : files) {
      long inStart = System.currentTimeMillis();
      try {
        List<String[]> linesList = FileParser.parseFileLinesFiles(file);
        sendIoTDBRequests(linesList, partition, session, schemaList);
      } catch (IOException | StatementExecutionException e) {
        logger.error("write error!", e);
      }
      long inEnd = System.currentTimeMillis();
      logger.info(
          "successfully deal {}{},file:{},sum:{},cost:{}s.",
          database,
          partition,
          file,
          set.size(),
          (inEnd - inStart) / 1000);
    }
    long outEnd = System.currentTimeMillis();
    logger.info(
        "successfully deal {}-{},sum:{},cost:{}s.",
        database,
        partition,
        set.size(),
        (outEnd - outStart) / 1000);
    countDownLatch.countDown();
  }

  public void sendIoTDBRequests(
      List<String[]> values, String partition, Session session, List<MeasurementSchema> schemaList)
      throws IOException, IoTDBConnectionException, StatementExecutionException {
    Tablet tablet = new Tablet(database + partition, schemaList, 1000);
    for (String[] parts : values) {
      int rowIndex = tablet.rowSize++;
      long time = convertToUnixTimestampMillis(parts[0] + " " + parts[1]);
      tablet.addTimestamp(rowIndex, time);
      for (int s = 2; s < parts.length; s++) {
        tablet.addValue(
            schemaList.get(s - 2).getMeasurementId(), rowIndex, Double.parseDouble(parts[s]));
      }
      set.add(time);
      if (tablet.rowSize == tablet.getMaxRowNumber()) {
        session.insertTablet(tablet, true);
        tablet.reset();
      }
    }

    if (tablet.rowSize != 0) {
      session.insertTablet(tablet);
      tablet.reset();
    }
  }

  public long convertToUnixTimestampMillis(String dateString) {

    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("'T'yyyy-MM-dd HH:mm:ss.SSSSSS");

    // 解析字符串为 LocalDateTime
    LocalDateTime dateTime = LocalDateTime.parse(dateString, formatter);

    // 转换为毫秒精度的时间戳
    return dateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  @Override
  public void run() {
    try {
      insert();
    } catch (IoTDBConnectionException e) {
      throw new RuntimeException(e);
    }
  }
}
