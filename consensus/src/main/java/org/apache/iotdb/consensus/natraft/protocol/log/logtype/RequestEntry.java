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

package org.apache.iotdb.consensus.natraft.protocol.log.logtype;

import org.apache.iotdb.consensus.common.request.ByteBufferConsensusRequest;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.tsfile.utils.PublicBAOS;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import static org.apache.iotdb.consensus.natraft.protocol.log.Entry.Types.CLIENT_REQUEST;

/** RequestLog contains a non-partitioned request like set storage group. */
public class RequestEntry extends Entry {

  private static final Logger logger = LoggerFactory.getLogger(RequestEntry.class);
  private IConsensusRequest request;

  public RequestEntry() {}

  public RequestEntry(IConsensusRequest request) {
    this.request = request;
  }

  @Override
  public ByteBuffer serialize() {
    PublicBAOS byteArrayOutputStream = new PublicBAOS(getDefaultSerializationBufferSize());
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      dataOutputStream.writeByte((byte) CLIENT_REQUEST.ordinal());

      dataOutputStream.writeLong(getCurrLogIndex());
      dataOutputStream.writeLong(getCurrLogTerm());

      ByteBuffer byteBuffer = request.serializeToByteBuffer();
      byteBuffer.rewind();
      dataOutputStream.writeInt(byteBuffer.remaining());
      dataOutputStream.write(
          byteBuffer.array(), byteBuffer.arrayOffset(), byteBuffer.remaining());
    } catch (IOException e) {
      // unreachable
    }

    return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
  }

  @Override
  public void serialize(ByteBuffer buffer) {
    buffer.put((byte) CLIENT_REQUEST.ordinal());
    buffer.putLong(getCurrLogIndex());
    buffer.putLong(getCurrLogTerm());
    ByteBuffer byteBuffer = request.serializeToByteBuffer();
    buffer.putInt(byteBuffer.remaining());
    buffer.put(byteBuffer);
  }

  @Override
  public void deserialize(ByteBuffer buffer) {
    setCurrLogIndex(buffer.getLong());
    setCurrLogTerm(buffer.getLong());
    int len = buffer.getInt();
    byte[] bytes = new byte[len];
    buffer.get(bytes);

    request = new ByteBufferConsensusRequest(ByteBuffer.wrap(bytes));
  }

  public IConsensusRequest getRequest() {
    return request;
  }

  public void setRequest(IConsensusRequest request) {
    this.request = request;
  }

  @Override
  public String toString() {
    return request + ",term:" + getCurrLogTerm() + ",index:" + getCurrLogIndex();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    RequestEntry that = (RequestEntry) o;
    return Objects.equals(request, that.request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), request);
  }

  @Override
  public long estimateSize() {
    return request.estimateSize();
  }
}