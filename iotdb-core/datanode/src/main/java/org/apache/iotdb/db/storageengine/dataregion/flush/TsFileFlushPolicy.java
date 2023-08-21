/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.apache.iotdb.db.storageengine.dataregion.DataRegion;
import org.apache.iotdb.db.storageengine.dataregion.memtable.TsFileProcessor;

/**
 * TsFileFlushPolicy is applied when a TsFileProcessor is full after insertion. For standalone
 * IoTDB, the flush or close is executed without constraint. But in the distributed version, the
 * close is controlled by the leader and should not be performed by the follower alone.
 */
public interface TsFileFlushPolicy {

  void apply(DataRegion dataRegion, TsFileProcessor processor, boolean isSeq);

  class DirectFlushPolicy implements TsFileFlushPolicy {

    @Override
    public void apply(DataRegion dataRegion, TsFileProcessor tsFileProcessor, boolean isSeq) {
      if (tsFileProcessor.shouldClose()) {
        // 一定会走到这里，shouldClose中的fileSizeThreshold为0且不可改
        dataRegion.asyncCloseOneTsFileProcessor(isSeq, tsFileProcessor);
      } else {
        tsFileProcessor.asyncFlush();
      }
    }
  }
}
