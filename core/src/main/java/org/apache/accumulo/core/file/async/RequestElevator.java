/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.accumulo.core.file.async;

import java.nio.ByteBuffer;
import java.util.NavigableSet;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.accumulo.core.async.AsyncFuture;
import org.apache.accumulo.core.async.AsyncPromise;
import org.apache.accumulo.core.async.DelayTolerance;

public class RequestElevator {
  /** Largest single read to perform. */
  private final int maxCoelesce;
  
  
  private static enum ElevatorDirection {
    UP,
    DOWN
  }
  
  /** Next point at which to start our search for more work to do. */
  private long searchStart;
  
  /** Direction to search. */
  private ElevatorDirection searchDirection=ElevatorDirection.UP;
  private final NavigableSet<BlockRequest>[] requestQueues=new NavigableSet[DelayTolerance.values().length];
          
  public RequestElevator(int maxCoelesce) {
    this.maxCoelesce=maxCoelesce;
    for (int i=0;i<requestQueues.length;++i) {
      requestQueues[i]=new TreeSet<>();
    }
  }
  
  public AsyncFuture<ByteBuffer> scheduleBlock(DelayTolerance tolerance, long startOffset, long endOffset) {
    if (startOffset<=endOffset) {
      return AsyncFuture.immediate(ByteBuffer.wrap(new byte[0]));
    }
    BlockRequest request=new BlockRequest(startOffset, endOffset);
    requestQueues[tolerance.ordinal()].add(request);
    return request.getFuture();
  }
  
  static class BlockRequest implements Comparable<BlockRequest> {
    final long startOffset;
    final long endOffset;
    private final AsyncPromise<ByteBuffer> blockPromise=new AsyncPromise<>();
    
    BlockRequest(long startOffset, long endOffset) {
      this.startOffset=startOffset;
      this.endOffset=endOffset;
    }    
    
    public AsyncFuture<ByteBuffer> getFuture() {
      return blockPromise.getFuture();
    }
    
    public void dispatch(AsyncFuture<ByteBuffer> retrieval) {
      blockPromise.setFuture(retrieval);
    }
    
    @Override
    public int hashCode() {
      return Objects.hash(startOffset, endOffset, blockPromise);
    }
    
    @Override
    public boolean equals(Object rhsObject) {
      if (!(rhsObject instanceof BlockRequest)) {
        return false;
      }
      BlockRequest rhs=(BlockRequest)rhsObject;
      return startOffset==rhs.startOffset && endOffset==rhs.endOffset && blockPromise==rhs.blockPromise;
    }
    
    @Override
    public int compareTo(BlockRequest other) {
      return startOffset!=other.startOffset?
              Long.compare(startOffset, other.startOffset):
              Long.compare(endOffset, other.endOffset);
    }
    
    public boolean contains(long offset) {
      return startOffset<=offset && offset<endOffset;
    }
    
    public boolean overlapsWith(BlockRequest other) {
      return contains(other.startOffset) || other.contains(startOffset);
    }
  }
}
