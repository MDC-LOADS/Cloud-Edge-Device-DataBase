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

package org.apache.iotdb.db.queryengine.execution.operator.source;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.*;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Binary;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SeriesScanOperator extends AbstractDataSourceOperator {

  private final TsBlockBuilder builder;
  private boolean finished = false;
  private int fragmentId;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISinkHandle sinkHandle;

  public SeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
  }
  public SeriesScanOperator(
          OperatorContext context,
          PlanNodeId sourceId,
          PartialPath seriesPath,
          Ordering scanOrder,
          SeriesScanOptions seriesScanOptions,
          int fragmentId) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
            new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext());
    this.maxReturnSize =
            Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
    this.fragmentId=fragmentId;
    final String queryId = "test_query_"+sourceId.getId();
//    final String queryId_s = "test_query_s_"+sourceId.getId();
    final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getEdgeFragmentId(), "0");
    final String remotePlanNodeId = "receive_test_"+sourceId.getId();
    final String localPlanNodeId = "send_test_"+sourceId.getId();
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentId, "0");
    int channelNum = 1;
    AtomicInteger cnt = new AtomicInteger(channelNum);
    long query_num=1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    DownStreamChannelIndex downStreamChannelIndex = new DownStreamChannelIndex(0);
    sinkHandle =
            MPP_DATA_EXCHANGE_MANAGER.createShuffleSinkHandle(
                    Collections.singletonList(
                            new DownStreamChannelLocation(
                                    remoteEndpoint,
                                    remoteFragmentInstanceId,
                                    remotePlanNodeId)),
                    downStreamChannelIndex,
                    ShuffleSinkHandle.ShuffleStrategyEnum.PLAIN,
                    localFragmentInstanceId,
                    localPlanNodeId,
                    instanceContext);
    PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setSinkHandle(this.sinkHandle);
    if(PipeInfo.getInstance().getPipeStatus()){
      sinkHandle.tryOpenChannel(0);
    }
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    resultTsBlock = builder.build();
    if(PipeInfo.getInstance().getPipeStatus()){
//      Column[] valueColumns = resultTsBlock.getValueColumns();
//      System.out.println("result columns binary:");
//      Binary[] binaryColumn=valueColumns[0].getBinaries();
//      for(Binary binaryObject:binaryColumn){
//        System.out.println(binaryObject);
//      }
//      TimeColumn timeColumn=resultTsBlock.getTimeColumn();
//      long[] times=timeColumn.getTimes();
//      System.out.println("result time columns:");
//      for(long time:times){
//        System.out.println(time);
//      }
      System.out.println("localfragmentid:"+fragmentId);
      System.out.println("remoteid:"+PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getEdgeFragmentId());
      sinkHandle.send(resultTsBlock);//发送数据
//      System.out.println("isNoMoreTs:"+ sinkHandle.getChannel(0).isNoMoreTsBlocks());

//      sinkHandle.wait();
//      while(sinkHandle.getChannel(0).getNumOfBufferedTsBlocks()!=0){
//        try {
//                Thread.sleep(10);
//        //          System.out.println("waiting");
//              } catch (InterruptedException e) {
//                throw new RuntimeException(e);
//              }
//      }

    }
    builder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    try {

      // start stopwatch
      long maxRuntime = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
      long start = System.nanoTime();

      // here use do-while to promise doing this at least once
      do {
        /*
         * 1. consume page data firstly
         * 2. consume chunk data secondly
         * 3. consume next file finally
         */
        if (!readPageData() && !readChunkData() && !readFileData()) {
          break;
        }
      } while (System.nanoTime() - start < maxRuntime && !builder.isFull());

      finished = builder.isEmpty();
      System.out.println("finished="+finished);
      if(finished && PipeInfo.getInstance().getPipeStatus()){
        while(sinkHandle.getChannel(0).getNumOfBufferedTsBlocks()!=0){
          try {
            Thread.sleep(10);
            //          System.out.println("waiting");
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
        sinkHandle.setNoMoreTsBlocksOfOneChannel(0);
        sinkHandle.close();
        System.out.println("close finished");

      }

      return !finished;
    } catch (IOException e) {
      throw new RuntimeException("Error happened while scanning the file", e);
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    return finished;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return Math.max(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return calculateMaxPeekMemory() - calculateMaxReturnSize();
  }

  private boolean readFileData() throws IOException {
    while (seriesScanUtil.hasNextFile()) {
      if (readChunkData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readChunkData() throws IOException {
    while (seriesScanUtil.hasNextChunk()) {
      if (readPageData()) {
        return true;
      }
    }
    return false;
  }

  private boolean readPageData() throws IOException {
    while (seriesScanUtil.hasNextPage()) {
      TsBlock tsBlock = seriesScanUtil.nextPage();

      if (!isEmpty(tsBlock)) {
        appendToBuilder(tsBlock);
        return true;
      }
    }
    return false;
  }

  private void appendToBuilder(TsBlock tsBlock) {
    TimeColumnBuilder timeColumnBuilder = builder.getTimeColumnBuilder();
    TimeColumn timeColumn = tsBlock.getTimeColumn();
    ColumnBuilder columnBuilder = builder.getColumnBuilder(0);
    Column column = tsBlock.getColumn(0);

    if (column.mayHaveNull()) {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        if (column.isNull(i)) {
          columnBuilder.appendNull();
        } else {
          columnBuilder.write(column, i);
        }
        builder.declarePosition();
      }
    } else {
      for (int i = 0, size = tsBlock.getPositionCount(); i < size; i++) {
        timeColumnBuilder.writeLong(timeColumn.getLong(i));
        columnBuilder.write(column, i);
        builder.declarePosition();
      }
    }
  }

  private boolean isEmpty(TsBlock tsBlock) {
    return tsBlock == null || tsBlock.isEmpty();
  }
}
