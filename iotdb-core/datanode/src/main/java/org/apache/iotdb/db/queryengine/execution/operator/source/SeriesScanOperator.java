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

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;

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
  private int testflag=0;
  private int CloudFragmentId=0;


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
//    System.out.println("receiver start");
  }

  @Override
  public TsBlock next() throws Exception {
    if (retainedTsBlock != null) {
      return getResultFromRetainedTsBlock();
    }
    PipeInfo pipeInfo=PipeInfo.getInstance();
//    System.out.println("----status:"+pipeInfo.getPipeStatus()+"---scanstatus:"+pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isStatus());
    if(pipeInfo.getPipeStatus() && pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()){
      //pipe开启且当前scan算子pipe开启且builder内为空
//      System.out.println("---in---");
//      System.out.println("---localfragmentid:"+fragmentId);
//      System.out.println("remote:"+PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).getCloudFragmentId());
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
        appendToBuilder(tsBlock_rev);
      }else{
        //如果关闭了，需要重新启动scanUtil
        if(pipeInfo.getPipeStatus()){
          //还在启动，说明是数据查没了
          finished=true;
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          sourceHandle=null;
          return tsBlock_rev;
        }else{
          //需要重新构建读取器
          seriesScanBuilder.withOffset(pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).getOffset());
          seriesScanUtil=new SeriesScanUtil(seriesPath, scanOrder, seriesScanBuilder.build(), operatorContext.getInstanceContext(),Integer.parseInt(sourceId.getId()));
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setStatus(false);
          pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setSetOffset(false);
          sourceHandle=null;
          return tsBlock_rev;
        }

      }
    }
    resultTsBlock = builder.build();
    offset+=resultTsBlock.getPositionCount();//当前查询结果

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
    pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setOffset(offset);
//    if(pipeInfo.getPipeStatus()){//如果pipe开启就一直设置offset
//        pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).setOffset(offset);
//    }
//    if(pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).isSetOffset()){
//      System.out.println("offset:"+pipeInfo.getScanStatus(Integer.parseInt(sourceId.getId())).getOffset());
//    }
//    System.out.println("offset:"+offset);
    builder.reset();
    return checkTsBlockSizeAndGetResult();
  }

  @SuppressWarnings("squid:S112")
  @Override
  public boolean hasNext() throws Exception {
//    System.out.println("----hasNext()----");
    if (retainedTsBlock != null) {
      return true;
    }
//    if(testflag==0){
//      PipeInfo.getInstance().setPipeStatus(true);
//      TTransport transport = null;
//      try  {
//        transport =  new TFramedTransport(new TSocket("localhost", 9091));
//        TProtocol protocol = new TBinaryProtocol(transport);
//        PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
//        transport.open();
//        // 调用服务方法
//        client.PipeStart(PipeInfo.getInstance().getSql());
////        System.out.println("start successfully.");
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
    if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()){
      if(sourceHandle!=null&&sourceHandle.isFinished()){
        finished=true;
      }else{
        finished=false;//如果已经打开通道开始传输数据了，返回还有数据
      }
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
              PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).setOldFragmentId(CloudFragmentId);
//              System.out.println("set"+sourceId.getId()+"true");
            }
            break;
          }
        } while (System.nanoTime() - start < maxRuntime && !builder.isFull());

        finished = builder.isEmpty();
        if(PipeInfo.getInstance().getPipeStatus() && PipeInfo.getInstance().getScanStatus(Integer.parseInt(sourceId.getId())).isStatus()) {
          if(sourceHandle!=null&&sourceHandle.isFinished()){
            finished=true;
          }else{
            finished=false;//如果已经打开通道开始传输数据了，返回还有数据
          }
          return !finished;
        }
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
    if(tsBlock==null){
      return;
    }
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
