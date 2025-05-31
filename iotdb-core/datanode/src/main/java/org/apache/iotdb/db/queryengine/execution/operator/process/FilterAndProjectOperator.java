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

package org.apache.iotdb.db.queryengine.execution.operator.process;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelIndex;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.DownStreamChannelLocation;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ShuffleSinkHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.operator.Operator;
import org.apache.iotdb.db.queryengine.execution.operator.OperatorContext;
import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.queryengine.transformation.dag.column.CaseWhenThenColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.binary.BinaryColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.IdentityColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.leaf.LeafColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.multi.MappableUDFColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.ternary.TernaryColumnTransformer;
import org.apache.iotdb.db.queryengine.transformation.dag.column.unary.UnaryColumnTransformer;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.TsBlockBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.ColumnBuilder;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumnBuilder;
import org.apache.iotdb.tsfile.utils.Pair;

import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class FilterAndProjectOperator implements ProcessOperator {

  private final Operator inputOperator;

  private final List<LeafColumnTransformer> filterLeafColumnTransformerList;

  private final ColumnTransformer filterOutputTransformer;

  private final List<ColumnTransformer> commonTransformerList;

  private final List<LeafColumnTransformer> projectLeafColumnTransformerList;

  private final List<ColumnTransformer> projectOutputTransformerList;

  private final TsBlockBuilder filterTsBlockBuilder;

  private final boolean hasNonMappableUDF;

  private final OperatorContext operatorContext;

  // false when we only need to do projection
  private final boolean hasFilter;

  private int fragmentId;
  private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
          MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
  private ISinkHandle sinkHandle;

  @SuppressWarnings("squid:S107")
  public FilterAndProjectOperator(
      OperatorContext operatorContext,
      Operator inputOperator,
      List<TSDataType> filterOutputDataTypes,
      List<LeafColumnTransformer> filterLeafColumnTransformerList,
      ColumnTransformer filterOutputTransformer,
      List<ColumnTransformer> commonTransformerList,
      List<LeafColumnTransformer> projectLeafColumnTransformerList,
      List<ColumnTransformer> projectOutputTransformerList,
      boolean hasNonMappableUDF,
      boolean hasFilter) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.filterTsBlockBuilder = new TsBlockBuilder(8, filterOutputDataTypes);
    this.hasFilter = hasFilter;
  }
  public FilterAndProjectOperator(
          OperatorContext operatorContext,
          Operator inputOperator,
          List<TSDataType> filterOutputDataTypes,
          List<LeafColumnTransformer> filterLeafColumnTransformerList,
          ColumnTransformer filterOutputTransformer,
          List<ColumnTransformer> commonTransformerList,
          List<LeafColumnTransformer> projectLeafColumnTransformerList,
          List<ColumnTransformer> projectOutputTransformerList,
          boolean hasNonMappableUDF,
          boolean hasFilter,
          int sourceId,
          int fragmentId) {
    this.operatorContext = operatorContext;
    this.inputOperator = inputOperator;
    this.filterLeafColumnTransformerList = filterLeafColumnTransformerList;
    this.filterOutputTransformer = filterOutputTransformer;
    this.commonTransformerList = commonTransformerList;
    this.projectLeafColumnTransformerList = projectLeafColumnTransformerList;
    this.projectOutputTransformerList = projectOutputTransformerList;
    this.hasNonMappableUDF = hasNonMappableUDF;
    this.filterTsBlockBuilder = new TsBlockBuilder(8, filterOutputDataTypes);
    this.hasFilter = hasFilter;
    if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().isFilter()){
      final String queryId = "test_query_"+sourceId;
//    final String queryId_s = "test_query_s_"+sourceId.getId();
      final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10744);
      final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, PipeInfo.getInstance().getScanStatus(sourceId).getEdgeFragmentId(), "0");
      final String remotePlanNodeId = "receive_test_"+sourceId;
      final String localPlanNodeId = "send_test_"+sourceId;
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
      PipeInfo.getInstance().getScanStatus(sourceId).setSinkHandle(this.sinkHandle);

      sinkHandle.tryOpenChannel(0);
    }
  }
  @Override
  public OperatorContext getOperatorContext() {
    return operatorContext;
  }

  @Override
  public TsBlock next() throws Exception {
    TsBlock input = inputOperator.nextWithTimer();
    if (input == null) {
      return null;
    }

    if (!hasFilter) {
      return getTransformedTsBlock(input);
    }

    TsBlock filterResult = getFilterTsBlock(input);
    if(PipeInfo.getInstance().getPipeStatus() &&  PipeInfo.getInstance().isFilter()){
      if(filterResult.getPositionCount()!=0 && !sinkHandle.isAborted()){
        try {
          Thread.sleep(2);
          //          System.out.println("waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }

        sinkHandle.send(filterResult);//发送数据
        System.out.println("filter scan send");
      }
    }
    // contains non-mappable udf, we leave calculation for TransformOperator
    if (hasNonMappableUDF) {
      return filterResult;
    }
    return getTransformedTsBlock(filterResult);
  }

  /**
   * Return the TsBlock that contains both initial input columns and columns of common
   * subexpressions after filtering.
   */
  private TsBlock getFilterTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed Filter ColumnTransformer, including TimeStampColumnTransformer and constant
    for (LeafColumnTransformer leafColumnTransformer : filterLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    filterOutputTransformer.tryEvaluate();

    Column filterColumn = filterOutputTransformer.getColumn();

    // reuse this builder
    filterTsBlockBuilder.reset();

    final TimeColumnBuilder timeBuilder = filterTsBlockBuilder.getTimeColumnBuilder();
    final ColumnBuilder[] columnBuilders = filterTsBlockBuilder.getValueColumnBuilders();

    List<Column> resultColumns = new ArrayList<>();
    for (int i = 0, n = input.getValueColumnCount(); i < n; i++) {
      resultColumns.add(input.getColumn(i));
    }

    if (!hasNonMappableUDF) {
      // get result of calculated common sub expressions
      for (ColumnTransformer columnTransformer : commonTransformerList) {
        resultColumns.add(columnTransformer.getColumn());
      }
    }

    int rowCount =
        constructFilteredTsBlock(
            resultColumns,
            timeBuilder,
            filterColumn,
            originTimeColumn,
            columnBuilders,
            positionCount);

    filterTsBlockBuilder.declarePositions(rowCount);
    return filterTsBlockBuilder.build();
  }

  private int constructFilteredTsBlock(
      List<Column> resultColumns,
      TimeColumnBuilder timeBuilder,
      Column filterColumn,
      TimeColumn originTimeColumn,
      ColumnBuilder[] columnBuilders,
      int positionCount) {
    // construct result TsBlock of filter
    int rowCount = 0;
    for (int i = 0, n = resultColumns.size(); i < n; i++) {
      Column curColumn = resultColumns.get(i);
      for (int j = 0; j < positionCount; j++) {
        if (satisfy(filterColumn, j)) {
          if (i == 0) {
            rowCount++;
            timeBuilder.writeLong(originTimeColumn.getLong(j));
          }
          if (curColumn.isNull(j)) {
            columnBuilders[i].appendNull();
          } else {
            columnBuilders[i].write(curColumn, j);
          }
        }
      }
    }
    return rowCount;
  }

  private boolean satisfy(Column filterColumn, int rowIndex) {
    return !filterColumn.isNull(rowIndex) && filterColumn.getBoolean(rowIndex);
  }

  private TsBlock getTransformedTsBlock(TsBlock input) {
    final TimeColumn originTimeColumn = input.getTimeColumn();
    final int positionCount = originTimeColumn.getPositionCount();
    // feed pre calculated data
    for (LeafColumnTransformer leafColumnTransformer : projectLeafColumnTransformerList) {
      leafColumnTransformer.initFromTsBlock(input);
    }

    List<Column> resultColumns = new ArrayList<>();
    for (ColumnTransformer columnTransformer : projectOutputTransformerList) {
      columnTransformer.tryEvaluate();
      resultColumns.add(columnTransformer.getColumn());
    }
    return TsBlock.wrapBlocksWithoutCopy(
        positionCount, originTimeColumn, resultColumns.toArray(new Column[0]));
  }

  @Override
  public boolean hasNext() throws Exception {
    boolean unfinished=inputOperator.hasNextWithTimer();
    if(!unfinished && PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().isFilter()){
      while(sinkHandle.getChannel(0).getNumOfBufferedTsBlocks()!=0){
        try {
          Thread.sleep(10);//时间
          //          System.out.println("waiting");
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      }
      sinkHandle.setNoMoreTsBlocksOfOneChannel(0);
      sinkHandle.close();
      System.out.println("close finished");

    }
    return unfinished;
  }

  @Override
  public boolean isFinished() throws Exception {
    return inputOperator.isFinished();
  }

  @Override
  public ListenableFuture<?> isBlocked() {
    return inputOperator.isBlocked();
  }

  @Override
  public void close() throws Exception {
    for (ColumnTransformer columnTransformer : projectOutputTransformerList) {
      columnTransformer.close();
    }
    if (filterOutputTransformer != null) {
      filterOutputTransformer.close();
    }
    inputOperator.close();
  }

  @Override
  public long calculateMaxPeekMemory() {
    long maxPeekMemory = inputOperator.calculateMaxReturnSize();
    int maxCachedColumn = 0;
    // Only do projection, calculate max cached column size of calc tree
    if (!hasFilter) {
      for (int i = 0; i < projectOutputTransformerList.size(); i++) {
        ColumnTransformer c = projectOutputTransformerList.get(i);
        maxCachedColumn = Math.max(maxCachedColumn, 1 + i + getMaxLevelOfColumnTransformerTree(c));
      }
      return Math.max(
              maxPeekMemory,
              (long) maxCachedColumn
                  * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte())
          + inputOperator.calculateRetainedSizeAfterCallingNext();
    }

    // has Filter
    maxCachedColumn =
        Math.max(
            1 + getMaxLevelOfColumnTransformerTree(filterOutputTransformer),
            1 + commonTransformerList.size());
    if (!hasNonMappableUDF) {
      for (int i = 0; i < projectOutputTransformerList.size(); i++) {
        ColumnTransformer c = projectOutputTransformerList.get(i);
        maxCachedColumn = Math.max(maxCachedColumn, 1 + i + getMaxLevelOfColumnTransformerTree(c));
      }
    }
    return Math.max(
            maxPeekMemory,
            (long) maxCachedColumn * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte())
        + inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  @Override
  public long calculateMaxReturnSize() {
    // time + all value columns
    if (!hasFilter || !hasNonMappableUDF) {
      return (long) (1 + projectOutputTransformerList.size())
          * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    } else {
      return (long) (1 + filterTsBlockBuilder.getValueColumnBuilders().length)
          * TSFileDescriptor.getInstance().getConfig().getPageSizeInByte();
    }
  }

  @Override
  public long calculateRetainedSizeAfterCallingNext() {
    return inputOperator.calculateRetainedSizeAfterCallingNext();
  }

  private int getMaxLevelOfColumnTransformerTree(ColumnTransformer columnTransformer) {
    if (columnTransformer instanceof LeafColumnTransformer) {
      // Time column is always calculated, we ignore it here. Constant column is ignored.
      if (columnTransformer instanceof IdentityColumnTransformer) {
        return 1;
      } else {
        return 0;
      }
    } else if (columnTransformer instanceof UnaryColumnTransformer) {
      return Math.max(
          2,
          getMaxLevelOfColumnTransformerTree(
              ((UnaryColumnTransformer) columnTransformer).getChildColumnTransformer()));
    } else if (columnTransformer instanceof BinaryColumnTransformer) {
      int childMaxLevel =
          Math.max(
              getMaxLevelOfColumnTransformerTree(
                  ((BinaryColumnTransformer) columnTransformer).getLeftTransformer()),
              getMaxLevelOfColumnTransformerTree(
                  ((BinaryColumnTransformer) columnTransformer).getRightTransformer()));
      return Math.max(3, childMaxLevel);
    } else if (columnTransformer instanceof TernaryColumnTransformer) {
      int childMaxLevel =
          Math.max(
              getMaxLevelOfColumnTransformerTree(
                  ((TernaryColumnTransformer) columnTransformer).getFirstColumnTransformer()),
              Math.max(
                  getMaxLevelOfColumnTransformerTree(
                      ((TernaryColumnTransformer) columnTransformer).getSecondColumnTransformer()),
                  getMaxLevelOfColumnTransformerTree(
                      ((TernaryColumnTransformer) columnTransformer).getThirdColumnTransformer())));
      return Math.max(4, childMaxLevel);
    } else if (columnTransformer instanceof MappableUDFColumnTransformer) {
      int childMaxLevel = 0;
      for (ColumnTransformer c :
          ((MappableUDFColumnTransformer) columnTransformer).getInputColumnTransformers()) {
        childMaxLevel = Math.max(childMaxLevel, getMaxLevelOfColumnTransformerTree(c));
      }
      return Math.max(
          1
              + ((MappableUDFColumnTransformer) columnTransformer)
                  .getInputColumnTransformers()
                  .length,
          childMaxLevel);
    } else if (columnTransformer instanceof CaseWhenThenColumnTransformer) {
      int childMaxLevel = 0;
      int childCount = 0;
      for (Pair<ColumnTransformer, ColumnTransformer> whenThenColumnTransformer :
          ((CaseWhenThenColumnTransformer) columnTransformer).getWhenThenColumnTransformers()) {
        childMaxLevel =
            Math.max(
                childMaxLevel, getMaxLevelOfColumnTransformerTree(whenThenColumnTransformer.left));
        childMaxLevel =
            Math.max(
                childMaxLevel, getMaxLevelOfColumnTransformerTree(whenThenColumnTransformer.right));
        childCount++;
      }
      childMaxLevel =
          Math.max(
              childMaxLevel,
              getMaxLevelOfColumnTransformerTree(
                  ((CaseWhenThenColumnTransformer) columnTransformer).getElseTransformer()));
      childMaxLevel = Math.max(childMaxLevel, childCount + 2);
      return childMaxLevel;
    } else {
      throw new UnsupportedOperationException("Unsupported ColumnTransformer");
    }
  }
}
