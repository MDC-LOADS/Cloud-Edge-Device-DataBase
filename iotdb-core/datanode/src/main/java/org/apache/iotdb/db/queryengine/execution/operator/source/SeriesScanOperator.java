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

import com.google.common.util.concurrent.ListenableFuture;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
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

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class SeriesScanOperator extends AbstractDataSourceOperator {

  private final TsBlockBuilder builder;
  private boolean finished = false;
  private int fragmentId;
  private int offset=0;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISourceHandle sourceHandle;
  private SeriesScanOptions.Builder seriesScanBuilder;
  private PartialPath seriesPath;
  private Ordering scanOrder;


  public SeriesScanOperator(
      OperatorContext context,
      PlanNodeId sourceId,
      PartialPath seriesPath,
      Ordering scanOrder,
      SeriesScanOptions seriesScanOptions) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
        new SeriesScanUtil(seriesPath, scanOrder, seriesScanOptions, context.getInstanceContext(),Integer.parseInt(sourceId.getId()));
    this.maxReturnSize =
        Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
  }
  public SeriesScanOperator(
          OperatorContext context,
          PlanNodeId sourceId,
          PartialPath seriesPath,
          Ordering scanOrder,
          SeriesScanOptions.Builder seriesScanBuilder,
          int fragmentId) {
    this.seriesScanBuilder=seriesScanBuilder;
    SeriesScanOptions seriesScanOptions=this.seriesScanBuilder.build();
    this.seriesPath=seriesPath;
    this.scanOrder=scanOrder;
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.seriesScanUtil =
            new SeriesScanUtil(this.seriesPath, this.scanOrder, seriesScanOptions, context.getInstanceContext(),Integer.parseInt(sourceId.getId()));
    this.maxReturnSize =
            Math.min(maxReturnSize, TSFileDescriptor.getInstance().getConfig().getPageSizeInByte());
    this.builder = new TsBlockBuilder(seriesScanUtil.getTsDataTypeList());
    this.fragmentId = fragmentId;
    final String queryId_r = "test_query_r_"+sourceId.getId();
    final String queryId_s = "test_query_s_"+sourceId.getId();
    final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
    final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId_s, PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId(), "0");
    final String localPlanNodeId = "receive_test_"+sourceId.getId();
    final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId_r, fragmentId, "0");
    long query_num=1;
    FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
    this.sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
            localFragmentInstanceId,
            localPlanNodeId,
            0,//IndexOfUpstreamSinkHandle
            remoteEndpoint,
            remoteFragmentInstanceId,
            instanceContext::failed);
    final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
    sourceHandle.setMaxBytesCanReserve(MOCK_TSBLOCK_SIZE);
//    System.out.println("receiver start");
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    PipeInfo pipeInfo=PipeInfo.getInstance();
    if(pipeInfo.getPipeStatus() && pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isStatus() && builder.isEmpty()){
      //pipe开启且当前scan算子pipe开启且builder内为空
      ListenableFuture<?> isBlocked = sourceHandle.isBlocked();
      while (!isBlocked.isDone()) {
        try {
          Thread.sleep(10);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      TsBlock tsBlock_rev = null;
      if (!sourceHandle.isFinished()) {
        tsBlock_rev = sourceHandle.receive();
        appendToBuilder(tsBlock_rev);
      }else{
        //TODO:如果关闭了，需要重新启动scanUtil
        if(pipeInfo.getPipeStatus()){
          //还在启动，说明是数据查没了
          finished=true;
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          return tsBlock_rev;
        }else{
          //需要重新构建读取器
          seriesScanBuilder.withOffset(pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).getOffset());
          seriesScanUtil=new SeriesScanUtil(seriesPath, scanOrder, seriesScanBuilder.build(), operatorContext.getInstanceContext(),Integer.parseInt(sourceId.getId()));
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setSetOffset(false);
          return tsBlock_rev;
        }

      }
    }
    resultTsBlock = builder.build();
    offset+=resultTsBlock.getPositionCount();//当前查询结果


    if(pipeInfo.getPipeStatus()){//如果pipe开启就一直设置offset
      pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setOffset(offset);
    }
//    System.out.println("offset:"+offset);
    builder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
    if (retainedTsBlock != null) {
      return true;
    }
    if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()){
      finished=false;//如果已经打开通道开始传输数据了，返回还有数据
      return !finished;
    }else{
      try {
        if(finished){
          return !finished;
        }
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
            if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isSetOffset()){
              PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(true);
            }
            break;
          }
        } while (System.nanoTime() - start < maxRuntime && !builder.isFull());

        finished = builder.isEmpty();

        return !finished;
      } catch (IOException e) {
        throw new RuntimeException("Error happened while scanning the file", e);
      }
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
