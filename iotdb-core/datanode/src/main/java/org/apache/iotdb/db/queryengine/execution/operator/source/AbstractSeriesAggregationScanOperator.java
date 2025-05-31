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
import org.apache.iotdb.db.queryengine.execution.aggregation.Aggregator;
import org.apache.iotdb.db.queryengine.execution.aggregation.timerangeiterator.ITimeRangeIterator;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.GroupByTimeParameter;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.statistics.Statistics;
import org.apache.iotdb.tsfile.read.common.TimeRange;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.appendAggregationResult;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.calculateAggregationFromRawData;
import static org.apache.iotdb.db.queryengine.execution.operator.AggregationUtil.isAllAggregatorsHasFinalResult;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;

public abstract class AbstractSeriesAggregationScanOperator extends AbstractDataSourceOperator {

  protected final boolean ascending;
  protected final boolean isGroupByQuery;

  protected int subSensorSize;

  protected TsBlock inputTsBlock;

  protected final ITimeRangeIterator timeRangeIterator;
  // Current interval of aggregation window [curStartTime, curEndTime)
  protected TimeRange curTimeRange;

  // We still think aggregator in SeriesAggregateScanOperator is a inputRaw step.
  // But in facing of statistics, it will invoke another method processStatistics()
  protected final List<Aggregator> aggregators;

  // Using for building result tsBlock
  protected final TsBlockBuilder resultTsBlockBuilder;

  protected boolean finished = false;

  private final long cachedRawDataSize;

  /** Time slice for one next call in total, shared by the inner methods of the next() method */
  private long leftRuntimeOfOneNextCall;

  private int fragmentId;
  private int CloudFragmentId=0;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISourceHandle sourceHandle;
  private int testflag=0;

  @SuppressWarnings("squid:S107")
  protected AbstractSeriesAggregationScanOperator(
          PlanNodeId sourceId,
          OperatorContext context,
          SeriesScanUtil seriesScanUtil,
          int subSensorSize,
          List<Aggregator> aggregators,
          ITimeRangeIterator timeRangeIterator,
          boolean ascending,
          GroupByTimeParameter groupByTimeParameter,
          long maxReturnSize) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.isGroupByQuery = groupByTimeParameter != null;
    this.seriesScanUtil = seriesScanUtil;
    this.subSensorSize = subSensorSize;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.cachedRawDataSize =
            (1L + subSensorSize) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxReturnSize = maxReturnSize;
  }
  protected AbstractSeriesAggregationScanOperator(
          PlanNodeId sourceId,
          OperatorContext context,
          SeriesScanUtil seriesScanUtil,
          int subSensorSize,
          List<Aggregator> aggregators,
          ITimeRangeIterator timeRangeIterator,
          boolean ascending,
          GroupByTimeParameter groupByTimeParameter,
          long maxReturnSize,
          int fragmentId) {
    this.sourceId = sourceId;
    this.operatorContext = context;
    this.ascending = ascending;
    this.isGroupByQuery = groupByTimeParameter != null;
    this.seriesScanUtil = seriesScanUtil;
    this.subSensorSize = subSensorSize;
    this.aggregators = aggregators;
    this.timeRangeIterator = timeRangeIterator;

    List<TSDataType> dataTypes = new ArrayList<>();
    for (Aggregator aggregator : aggregators) {
      dataTypes.addAll(Arrays.asList(aggregator.getOutputType()));
    }
    this.resultTsBlockBuilder = new TsBlockBuilder(dataTypes);

    this.cachedRawDataSize =
            (1L + subSensorSize) * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    this.maxReturnSize = maxReturnSize;
    this.fragmentId = fragmentId;
  }

  @Override
  public long calculateMaxPeekMemory() {
    return cachedRawDataSize + maxReturnSize;
  }

  @Override
  public long calculateMaxReturnSize() {
    return maxReturnSize;
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return isGroupByQuery ? cachedRawDataSize : 0;
  }

  @Override
  public boolean hasNext() throws Exception {
//    if(testflag==0){
//      PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStartTime(timeRangeIterator.getFirstTimeRange().getMin());
//      PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setSetStartTime(true);
//      PipeInfo.getInstance().setPipeStatus(true);
//      if(PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isSetStartTime()){
//        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(true);
//        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setOldFragmentId(CloudFragmentId);
////              System.out.println("set"+sourceId.getId()+"true");
//      }
//      TTransport transport = null;
//      try  {
//        transport =  new TFramedTransport(new TSocket("localhost", 9091));
//        TProtocol protocol = new TBinaryProtocol(transport);
//        PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
//        transport.open();
//        // 调用服务方法
//        client.PipeStart(PipeInfo.getInstance().getSql());
//        System.out.println("start successfully.");
//
//      } catch (TException x) {
//        x.printStackTrace();
//      }finally {
//        if(null!=transport){
//          transport.close();
//        }
//      }
//      testflag=1;
//    }

    return curTimeRange != null || timeRangeIterator.hasNextTimeRange();
  }

  @Override
  public TsBlock next() throws Exception {

    // start stopwatch, reset leftRuntimeOfOneNextCall
    long start = System.nanoTime();
    leftRuntimeOfOneNextCall = operatorContext.getMaxRunTime().roundTo(TimeUnit.NANOSECONDS);
    long maxRuntime = leftRuntimeOfOneNextCall;
    TsBlock result = null;
    PipeInfo pipeInfo=PipeInfo.getInstance();
//    System.out.println("----status:"+pipeInfo.getPipeStatus()+"---scanstatus:"+pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isStatus());
    if(pipeInfo.getPipeStatus() && pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()){
      //pipe开启且当前scan算子pipe开启且builder内为空
      System.out.println("---in---");
      System.out.println("---localfragmentid:"+fragmentId);
      System.out.println("remote:"+PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId());
      String queryId = "test_query_"+sourceId.getId();
      TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
      while (PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId()==PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getOldFragmentId() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()){
        try {
          Thread.sleep(10);//时间
//          System.out.println("waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      if(CloudFragmentId!=PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId()){
        CloudFragmentId=PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId();
        TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId(), "0");
        String localPlanNodeId = "receive_test_"+sourceId.getId();
        TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, fragmentId, "0");
        long query_num=1;
        FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
//        System.out.println("cloudid:"+PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId());
        this.sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localFragmentInstanceId,
                localPlanNodeId,
                0,//IndexOfUpstreamSinkHandle
                remoteEndpoint,
                remoteFragmentInstanceId,
                instanceContext::failed);
        final long MOCK_TSBLOCK_SIZE = 1024L * 1024L;
        sourceHandle.setMaxBytesCanReserve(MOCK_TSBLOCK_SIZE);
      }
      ListenableFuture<?> isBlocked = sourceHandle.isBlocked();
      while (!isBlocked.isDone()&&!sourceHandle.isFinished()) {
        try {
          Thread.sleep(10);//时间
//          System.out.println("waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      TsBlock tsBlock_rev = null;
      if (!sourceHandle.isFinished()) {
        tsBlock_rev = sourceHandle.receive();
//        Column[] valueColumns = tsBlock_rev.getValueColumns();
//        System.out.println("receive columns binary:");
//        Binary[] binaryColumn=valueColumns[0].getBinaries();
//        for(Binary binaryObject:binaryColumn){
//          System.out.println(binaryObject);
//        }
//        TimeColumn timeColumn=tsBlock_rev.getTimeColumn();
//        long[] times=timeColumn.getTimes();
//        System.out.println("receive time columns:");
//        for(long time:times){
//          System.out.println(time);
//        }
        result=tsBlock_rev;
//        Column[] valueColumns = result.getValueColumns();
//        System.out.println("receive columns binary:");
//        float[] floatColumn=valueColumns[0].getFloats();
//        for(float floatObject:floatColumn){
//          System.out.println(floatObject);
//        }
//        TimeColumn timeColumn=result.getTimeColumn();
//        long[] times=timeColumn.getTimes();
//        System.out.println("receive time columns:");
//        for(long time:times){
//          System.out.println(time);
//        }
        for(int i=0;i<result.getPositionCount();i++){
          curTimeRange = timeRangeIterator.nextTimeRange();
        }
        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStartTime(curTimeRange.getMin());
        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setSetStartTime(true);
      }else{
        //如果关闭了，需要重新启动scanUtil
        if(pipeInfo.getPipeStatus()){
          //还在启动，说明是数据查没了
          finished=true;
          curTimeRange=timeRangeIterator.nextTimeRange();
//          System.out.println("curtime:"+curTimeRange+"time:"+timeRangeIterator.hasNextTimeRange());
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          sourceHandle=null;
          return tsBlock_rev;
        }else{
          //需要重新构建读取器

          while(pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).getStartTime()==timeRangeIterator.nextTimeRange().getMin()){
            curTimeRange = timeRangeIterator.nextTimeRange();
          }
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setSetStartTime(false);
          sourceHandle=null;
          return tsBlock_rev;
        }

      }
    }
    while (System.nanoTime() - start < maxRuntime
            && (curTimeRange != null || timeRangeIterator.hasNextTimeRange())
            && !resultTsBlockBuilder.isFull() && !pipeInfo.getPipeStatus()) {
      if (curTimeRange == null) {
        // move to the next time window
        curTimeRange = timeRangeIterator.nextTimeRange();
        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStartTime(curTimeRange.getMin());
        PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setSetStartTime(true);
        if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isSetStartTime()){
          PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(true);
          PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setOldFragmentId(CloudFragmentId);
//              System.out.println("set"+sourceId.getId()+"true");
        }
        // clear previous aggregation result
        for (Aggregator aggregator : aggregators) {
          aggregator.reset();
        }
      }

      // calculate aggregation result on current time window
      // Keep curTimeRange if the calculation of this timeRange is not done
      if (calculateAggregationResultForCurrentTimeRange()) {
        curTimeRange = null;
      }
    }

    if(result==null){
      if(resultTsBlockBuilder.getPositionCount() > 0){
        TsBlock resultTsBlock = resultTsBlockBuilder.build();
        resultTsBlockBuilder.reset();
        return resultTsBlock;
      }else {
        return null;
      }
    }
    if (resultTsBlockBuilder.getPositionCount() > 0 && result.getPositionCount()==0) {
      TsBlock resultTsBlock = resultTsBlockBuilder.build();
      resultTsBlockBuilder.reset();
      return resultTsBlock;
    } else if (result.getPositionCount()!=0) {
      return  result;
    } else {
      return null;
    }
  }

  @Override
  public boolean isFinished() throws Exception {
    if (!finished) {
      finished = !hasNextWithTimer();
    }
    return finished;
  }

  @SuppressWarnings("squid:S112")
  /** Return true if we have the result of this timeRange. */
  protected boolean calculateAggregationResultForCurrentTimeRange() {
    try {
      if (calcFromCachedData()) {
        updateResultTsBlock();
        return true;
      }

      if (readAndCalcFromPage()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page data has been consumed, we need to read the chunk data
      if (!seriesScanUtil.hasNextPage() && readAndCalcFromChunk()) {
        updateResultTsBlock();
        return true;
      }

      // only when all the page and chunk data has been consumed, we need to read the file data
      if (!seriesScanUtil.hasNextPage()
              && !seriesScanUtil.hasNextChunk()
              && readAndCalcFromFile()) {
        updateResultTsBlock();
        return true;
      }

      // If the TimeRange is (Long.MIN_VALUE, Long.MAX_VALUE), for Aggregators like countAggregator,
      // we have to consume all the data before we finish the aggregation calculation.
      if (seriesScanUtil.hasNextPage()
              || seriesScanUtil.hasNextChunk()
              || seriesScanUtil.hasNextFile()) {
        return false;
      }
      updateResultTsBlock();
      return true;
    } catch (IOException e) {
      throw new RuntimeException("Error while scanning the file", e);
    }
  }

  protected void updateResultTsBlock() {
    appendAggregationResult(
            resultTsBlockBuilder, aggregators, timeRangeIterator.currentOutputTime());
  }

  protected boolean calcFromCachedData() {
    return calcFromRawData(inputTsBlock);
  }

  private boolean calcFromRawData(TsBlock tsBlock) {
    Pair<Boolean, TsBlock> calcResult =
            calculateAggregationFromRawData(tsBlock, aggregators, curTimeRange, ascending);
    inputTsBlock = calcResult.getRight();
    return calcResult.getLeft();
  }

  protected void calcFromStatistics(Statistics timeStatistics, Statistics[] valueStatistics) {
    for (Aggregator aggregator : aggregators) {
      if (aggregator.hasFinalResult()) {
        continue;
      }
      aggregator.processStatistics(timeStatistics, valueStatistics);
    }
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromFile() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextFile()) {
      if (canUseCurrentFileStatistics()) {
        Statistics fileTimeStatistics = seriesScanUtil.currentFileTimeStatistics();
        if (fileTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentFile();
            continue;
          }
        }
        // calc from fileMetaData
        if (curTimeRange.contains(
                fileTimeStatistics.getStartTime(), fileTimeStatistics.getEndTime())) {
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentFileStatistics(i);
          }
          calcFromStatistics(fileTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentFile();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read chunk
      if (readAndCalcFromChunk()) {
        return true;
      }
    }

    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromChunk() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextChunk()) {
      if (canUseCurrentChunkStatistics()) {
        Statistics chunkTimeStatistics = seriesScanUtil.currentChunkTimeStatistics();
        if (chunkTimeStatistics.getStartTime() > curTimeRange.getMax()) {
          if (ascending) {
            return true;
          } else {
            seriesScanUtil.skipCurrentChunk();
            continue;
          }
        }
        // calc from chunkMetaData
        if (curTimeRange.contains(
                chunkTimeStatistics.getStartTime(), chunkTimeStatistics.getEndTime())) {
          // calc from chunkMetaData
          Statistics[] statisticsList = new Statistics[subSensorSize];
          for (int i = 0; i < subSensorSize; i++) {
            statisticsList[i] = seriesScanUtil.currentChunkStatistics(i);
          }
          calcFromStatistics(chunkTimeStatistics, statisticsList);
          seriesScanUtil.skipCurrentChunk();
          if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
            return true;
          } else {
            continue;
          }
        }
      }

      // read page
      if (readAndCalcFromPage()) {
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings({"squid:S3776", "squid:S135", "squid:S3740"})
  protected boolean readAndCalcFromPage() throws IOException {
    // start stopwatch
    long start = System.nanoTime();
    try {
      while (System.nanoTime() - start < leftRuntimeOfOneNextCall && seriesScanUtil.hasNextPage()) {
        if (canUseCurrentPageStatistics()) {
          Statistics pageTimeStatistics = seriesScanUtil.currentPageTimeStatistics();
          // There is no more eligible points in current time range
          if (pageTimeStatistics.getStartTime() > curTimeRange.getMax()) {
            if (ascending) {
              return true;
            } else {
              seriesScanUtil.skipCurrentPage();
              continue;
            }
          }
          // can use pageHeader
          if (curTimeRange.contains(
                  pageTimeStatistics.getStartTime(), pageTimeStatistics.getEndTime())) {
            Statistics[] statisticsList = new Statistics[subSensorSize];
            for (int i = 0; i < subSensorSize; i++) {
              statisticsList[i] = seriesScanUtil.currentPageStatistics(i);
            }
            calcFromStatistics(pageTimeStatistics, statisticsList);
            seriesScanUtil.skipCurrentPage();
            if (isAllAggregatorsHasFinalResult(aggregators) && !isGroupByQuery) {
              return true;
            } else {
              continue;
            }
          }
        }

        // calc from page data
        TsBlock tsBlock = seriesScanUtil.nextPage();
        if (tsBlock == null || tsBlock.isEmpty()) {
          continue;
        }

        // calc from raw data
        if (calcFromRawData(tsBlock)) {
          return true;
        }
      }
      return false;
    } finally {
      leftRuntimeOfOneNextCall -= (System.nanoTime() - start);
    }
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentFileStatistics() throws IOException {
    Statistics fileStatistics = seriesScanUtil.currentFileTimeStatistics();
    return !seriesScanUtil.isFileOverlapped()
            && fileStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
            && !seriesScanUtil.currentFileModified();
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentChunkStatistics() throws IOException {
    Statistics chunkStatistics = seriesScanUtil.currentChunkTimeStatistics();
    return !seriesScanUtil.isChunkOverlapped()
            && chunkStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
            && !seriesScanUtil.currentChunkModified();
  }

  @SuppressWarnings({"squid:S3740"})
  protected boolean canUseCurrentPageStatistics() throws IOException {
    Statistics currentPageStatistics = seriesScanUtil.currentPageTimeStatistics();
    if (currentPageStatistics == null) {
      return false;
    }
    return !seriesScanUtil.isPageOverlapped()
            && currentPageStatistics.containedByTimeFilter(seriesScanUtil.getGlobalTimeFilter())
            && !seriesScanUtil.currentPageModified();
  }
}
