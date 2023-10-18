/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.encoding.decoder.Decoder;
import org.apache.iotdb.tsfile.file.MetaMarker;
import org.apache.iotdb.tsfile.file.header.ChunkGroupHeader;
import org.apache.iotdb.tsfile.file.header.ChunkHeader;
import org.apache.iotdb.tsfile.file.header.PageHeader;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.fileSystem.FSFactoryProducer;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.BatchData;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;
import org.apache.iotdb.tsfile.read.reader.page.TimePageReader;
import org.apache.iotdb.tsfile.read.reader.page.ValuePageReader;
import org.apache.iotdb.tsfile.utils.TsPrimitiveType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** This tool is used to read TsFile sequentially, including nonAligned or aligned timeseries. */
public class TsFileVerifyTool {
  // if you wanna print detailed datas in pages, then turn it true.
  private static boolean printDetail = false;
  public static final String POINT_IN_PAGE = "\t\tpoints in the page: ";

  private static final Logger logger = LoggerFactory.getLogger(TsFileVerifyTool.class);


  @SuppressWarnings({
    "squid:S3776",
    "squid:S106"
  }) // Suppress high Cognitive Complexity and Standard outputs warning
  public static void main(String[] args) throws IOException {
    String filename = "/Users/caozhijia/Desktop/timecho/code/fork/master/iotdb/distribution/target/apache-iotdb-1.3.0-SNAPSHOT-all-bin/apache-iotdb-1.3.0-SNAPSHOT-all-bin/data/datanode/data/sequence/root.bw/1/2806/1697526162832-1-0-0.tsfile";
    if (args.length >= 1) {
      filename = args[0];
    }
    verifyTsFile(filename,1);

  }

  public static boolean verifyTsFile(String path,int level) {
    File file = new File(path);
    if (!file.exists()) {
      logger.error("this file is not exists:{}",path);
      return false;
    }


    try (TsFileSequenceReader reader = new TsFileSequenceReader(path)) {
      if (!reader.readHeadMagic().equals(TSFileConfig.MAGIC_STRING)) {
        logger.error("This file does not begin with TsFile");
        return false;
      }

      if (!reader.readTailMagic().equals(TSFileConfig.MAGIC_STRING)) {
        logger.error("This file does not end with TsFile");
        return false;
      }
      reader.position((long) TSFileConfig.MAGIC_STRING.getBytes().length + 1);
      System.out.println("position: " + reader.position());


//      reader.readRaw()


      List<long[]> timeBatch = new ArrayList<>();
      int pageIndex = 0;
      byte marker;
      while ((marker = reader.readMarker()) != MetaMarker.SEPARATOR) {
        switch (marker) {
          case MetaMarker.CHUNK_GROUP_HEADER:
            System.out.println("[Chunk Group]");
            System.out.println("Chunk Group Header position: " + reader.position());
            ChunkGroupHeader chunkGroupHeader = reader.readChunkGroupHeader();
            System.out.println("device: " + chunkGroupHeader.getDeviceID());

            System.out.println(reader.position());
            break;
          case MetaMarker.CHUNK_HEADER:
          case MetaMarker.TIME_CHUNK_HEADER:
          case MetaMarker.VALUE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_TIME_CHUNK_HEADER:
          case MetaMarker.ONLY_ONE_PAGE_VALUE_CHUNK_HEADER:
            System.out.println("\t[Chunk]");
            System.out.println("\tchunk type: " + marker);
            System.out.println("\tposition: " + reader.position());
            ChunkHeader header = reader.readChunkHeader(marker);
            System.out.println("\tMeasurement: " + header.getMeasurementID());
            if (header.getDataSize() == 0) {
              // empty value chunk
              System.out.println("\t-- Empty Chunk ");
              break;
            }
            System.out.println(
                    "\tChunk Size: " + (header.getDataSize() + header.getSerializedSize()));
            Decoder defaultTimeDecoder =
                    Decoder.getDecoderByType(
                            TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getTimeEncoder()),
                            TSDataType.INT64);
            Decoder valueDecoder =
                    Decoder.getDecoderByType(header.getEncodingType(), header.getDataType());
            int dataSize = header.getDataSize();
            pageIndex = 0;
            if (header.getDataType() == TSDataType.VECTOR) {
              timeBatch.clear();
            }
            while (dataSize > 0) {
              valueDecoder.reset();
              System.out.println(
                      "\t\t[Page" + pageIndex + "]\n \t\tPage head position: " + reader.position());
              PageHeader pageHeader =
                      reader.readPageHeader(
                              header.getDataType(),
                              (header.getChunkType() & 0x3F) == MetaMarker.CHUNK_HEADER);
              System.out.println("\t\tPage data position: " + reader.position());
              ByteBuffer pageData = reader.readPage(pageHeader, header.getCompressionType());
              System.out.println(
                      "\t\tUncompressed page data size: " + pageHeader.getUncompressedSize());
              System.out.println(
                      "\t\tCompressed page data size: " + pageHeader.getCompressedSize());
              if ((header.getChunkType() & TsFileConstant.TIME_COLUMN_MASK)
                      == TsFileConstant.TIME_COLUMN_MASK) { // Time Chunk
                TimePageReader timePageReader =
                        new TimePageReader(pageHeader, pageData, defaultTimeDecoder);
                timeBatch.add(timePageReader.getNextTimeBatch());
                System.out.println(POINT_IN_PAGE + timeBatch.get(pageIndex).length);
                if (printDetail) {
                  for (int i = 0; i < timeBatch.get(pageIndex).length; i++) {
                    System.out.println("\t\t\ttime: " + timeBatch.get(pageIndex)[i]);
                  }
                }
              } else if ((header.getChunkType() & TsFileConstant.VALUE_COLUMN_MASK)
                      == TsFileConstant.VALUE_COLUMN_MASK) { // Value Chunk
                ValuePageReader valuePageReader =
                        new ValuePageReader(pageHeader, pageData, header.getDataType(), valueDecoder);
                TsPrimitiveType[] valueBatch =
                        valuePageReader.nextValueBatch(timeBatch.get(pageIndex));
                if (valueBatch.length == 0) {
                  System.out.println("\t\t-- Empty Page ");
                } else {
                  System.out.println(POINT_IN_PAGE + valueBatch.length);
                }
                if (printDetail) {
                  for (TsPrimitiveType batch : valueBatch) {
                    System.out.println("\t\t\tvalue: " + batch);
                  }
                }
              } else { // NonAligned Chunk
                PageReader pageReader =
                        new PageReader(
                                pageData, header.getDataType(), valueDecoder, defaultTimeDecoder, null);
                BatchData batchData = pageReader.getAllSatisfiedPageData();
                if (header.getChunkType() == MetaMarker.CHUNK_HEADER) {
                  System.out.println(POINT_IN_PAGE + pageHeader.getNumOfValues());
                } else {
                  System.out.println(POINT_IN_PAGE + batchData.length());
                }
                if (printDetail) {
                  while (batchData.hasCurrent()) {
                    System.out.println(
                            "\t\t\ttime, value: "
                                    + batchData.currentTime()
                                    + ", "
                                    + batchData.currentValue());
                    batchData.next();
                  }
                }
              }
              pageIndex++;
              dataSize -= pageHeader.getSerializedPageSize();
            }
            break;

          case MetaMarker.OPERATION_INDEX_RANGE:
            reader.readPlanIndex();
            System.out.println("minPlanIndex: " + reader.getMinPlanIndex());
            System.out.println("maxPlanIndex: " + reader.getMaxPlanIndex());
            break;
          default:
            MetaMarker.handleUnexpectedMarker(marker);
        }
      }
      System.out.println("[Metadata]");
      for (String device : reader.getAllDevices()) {
        Map<String, List<ChunkMetadata>> seriesMetaData = reader.readChunkMetadataInDevice(device);
        System.out.printf(
                "\t[Device]Device %s, Number of Measurements %d%n", device, seriesMetaData.size());
        for (Map.Entry<String, List<ChunkMetadata>> serie : seriesMetaData.entrySet()) {
          System.out.println("\t\tMeasurement:" + serie.getKey());
          for (ChunkMetadata chunkMetadata : serie.getValue()) {
            System.out.println("\t\tFile offset:" + chunkMetadata.getOffsetOfChunkHeader());
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return false;
  }


}
