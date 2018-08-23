/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.dis;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import com.huaweicloud.dis.DIS;
import com.huaweicloud.dis.DISClientBuilder;
import com.huaweicloud.dis.exception.DISClientException;
import com.huaweicloud.dis.http.exception.ResourceAccessException;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequest;
import com.huaweicloud.dis.iface.data.request.PutRecordsRequestEntry;
import com.huaweicloud.dis.iface.data.response.PutRecordsResult;
import com.huaweicloud.dis.iface.data.response.PutRecordsResultEntry;

public class WikiGenerator {

  private static AtomicLong eventId = new AtomicLong(20000000);

  public static void main(String[] args) {
    if (args.length < 6) {
      System.err.println(
          "Usage: SensorProducer <stream name> <endpoint> <region> <ak> <sk> <project id> ");
      return;
    }

    DIS dic = DISClientBuilder.standard().withEndpoint(args[1]) .withAk(args[3]).withSk(args[4])
        .withProjectId(args[5]).withRegion(args[2]).build();

    Sensor sensor = new Sensor(dic, args[0]);
    Timer timer = new Timer();
    timer.schedule(sensor, 0, 100);

  }

  static class Sensor extends TimerTask {
    private DIS dic;

    private String streamName;

    private SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private Random random = new Random();

    private int i = 0;
    private int flag = 1;

    Sensor(DIS dic, String streamName) {
      this.dic = dic;
      this.streamName = streamName;
    }

    @Override public void run() {
      uploadData();
      // recordSensor();
    }

    private void uploadData() {
      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(streamName);
      List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<>();
      PutRecordsRequestEntry putRecordsRequestEntry = new PutRecordsRequestEntry();
      putRecordsRequestEntry.setData(ByteBuffer.wrap(recordSensorFromFile()));
      putRecordsRequestEntry
          .setPartitionKey(String.valueOf(ThreadLocalRandom.current().nextInt(1000000)));
      putRecordsRequestEntryList.add(putRecordsRequestEntry);
      putRecordsRequest.setRecords(putRecordsRequestEntryList);

      System.out.println("========== BEGIN PUT ============");

      PutRecordsResult putRecordsResult = null;
      try {
        putRecordsResult = dic.putRecords(putRecordsRequest);
      } catch (DISClientException e) {
        e.printStackTrace(System.err);
        System.err.println(
            "Failed to get a normal response, please check params and retry." + e.getMessage());
      } catch (ResourceAccessException e) {
        e.printStackTrace(System.err);
        System.err.println("Failed to access endpoint. " + e.getMessage());
      } catch (Exception e) {
        e.printStackTrace(System.err);
        System.err.println(e.getMessage());
      }

      if (putRecordsResult != null) {
        System.out.println(String.format("Put [%d] records [%d successful / %d failed]",
            putRecordsResult.getRecords().size(),
            putRecordsResult.getRecords().size() - putRecordsResult.getFailedRecordCount().get(),
            putRecordsResult.getFailedRecordCount().get()));
        for (int j = 0; j < putRecordsResult.getRecords().size(); j++) {
          PutRecordsResultEntry putRecordsRequestEntry1 = putRecordsResult.getRecords().get(j);
          if (putRecordsRequestEntry1.getErrorCode() != null) {
            System.out.println(String.format("[%s] put failed, errorCode [%s], errorMessage [%s]",
                new String(putRecordsRequestEntryList.get(j).getData().array(),
                    Charset.defaultCharset()),
                putRecordsRequestEntry1.getErrorCode(),
                putRecordsRequestEntry1.getErrorMessage()));
          } else {
            System.out.println(String.format(
                "[%s] put success, partitionId [%s], partitionKey [%s], sequenceNumber [%s]",
                new String(putRecordsRequestEntryList.get(j).getData().array(),
                    Charset.defaultCharset()),
                putRecordsRequestEntry1.getPartitionId(),
                putRecordsRequestEntryList.get(j).getPartitionKey(),
                putRecordsRequestEntry1.getSequenceNumber()));
          }
        }
      }
      System.out.println("========== END PUT ============");
    }

    private byte[] recordSensor() {
      StringBuilder builder = new StringBuilder();
      // event_id,building,device_id,record_time,temperature
      builder.append("2015,05,05,")
          .append(random.nextInt(24))
          .append(",en,carbon,")
          .append(random.nextInt(100))
          .append(",")
          .append(eventId.getAndIncrement());
      //      i = i + 1 * flag;
      //      if (i >= 5 || i <= 0) {
      //        flag = 0 - flag;
      //      }
      //      builder.append(",").append(25 + i).append(".").append(random.nextInt(10));
      System.out.println(builder.toString());
      return builder.toString().getBytes(Charset.defaultCharset());
    }


    private byte[] recordSensorFromFile() {
      StringBuilder builder = new StringBuilder();
      // event_id,building,device_id,record_time,temperature
      Date date = new Date();
      int year = date.getYear() + 1900;
      int month = date.getMonth() + 1;
      int day = date.getDate();
      int hour = date.getHours();
      int id = 20000000 + random.nextInt(1000000);

      Map typeMap = new HashMap();
      typeMap.put(0, "wikibooks");
      typeMap.put(1, "wiktionary");
      typeMap.put(2, "wikimedia");
      typeMap.put(3, "wikipedia mobile");
      typeMap.put(4, "wikinews");
      typeMap.put(5, "wikiquote");
      typeMap.put(6, "wikisource");
      typeMap.put(7, "wikiversity");
      typeMap.put(8, "mediawiki");

      StringBuffer title = new StringBuffer();
      title.append("carbon " + id);
      for (int i = 0; i < random.nextInt(10); i++) {
        title.append(" " + typeMap.getOrDefault(random.nextInt(9), "wiki"));
      }

      String time = new SimpleDateFormat("yyyyMMddHHmmssSSS").format(new Date());

      String delimiter = ",";
      builder.append(id).append(delimiter)
          .append(year).append(delimiter)
          .append(month).append(delimiter)
          .append(day).append(delimiter)
          .append(random.nextInt(hour)).append(delimiter)
          .append("en").append(delimiter)
          .append(typeMap.getOrDefault(random.nextInt(9), "wiki")).append(delimiter)
          .append(title).append(delimiter)
          .append(id * 10 + random.nextInt(5)).append(delimiter)
          .append(random.nextInt(1000)).append(delimiter)
          .append(random.nextInt(100000)).append(delimiter)
          .append(time)
      ;
      System.out.println(builder.toString());
      return builder.toString().getBytes(Charset.defaultCharset());
    }
  }
}
