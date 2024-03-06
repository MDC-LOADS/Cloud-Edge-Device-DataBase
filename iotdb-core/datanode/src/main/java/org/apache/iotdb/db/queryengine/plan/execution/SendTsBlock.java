package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkChannel;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.DoubleColumn;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class SendTsBlock {

    private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    public void send(){
        final String queryId = "test_query";
        final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
        final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
        final String remotePlanNodeId = "exchange_test";
        final String localPlanNodeId = "Sink_test";
        final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");

        int channelNum = 0;
        AtomicInteger cnt = new AtomicInteger(channelNum);
        long query_num=1;
        FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);

        SinkChannel sinkChannel = MPP_DATA_EXCHANGE_MANAGER.createSinkChannelcloud(
                localFragmentInstanceId,
                remoteEndpoint,
                remoteFragmentInstanceId,
                remotePlanNodeId,
                localPlanNodeId,
                instanceContext,
                cnt);

        sinkChannel.open();

        List<TsBlock> mockTsBlocks = new ArrayList<>();
        double[] values = new double[] {1.0, 2.0, 3.0, 5.0, 6.0, 8.0, 11.0, 13.0, 15.0, 17.0};
        double[] values2 = new double[] {4.0, 7.0, 9.0, 10.0, 12.0, 14.0, 16.0, 18.0, 19.0, 20.0};
        TimeColumn timeColumn2 = new TimeColumn(10, new long[] {4, 7, 9, 10, 12, 14, 16, 18, 19, 20});
        Column column2 = new DoubleColumn(10, Optional.empty(), values2);
        TimeColumn timeColumn = new TimeColumn(10, new long[] {1, 2, 3, 5, 6, 8, 11, 13, 15, 17});
        Column column = new DoubleColumn(10, Optional.empty(), values);
        TsBlock tsBlock = new TsBlock(timeColumn, column);
        TsBlock tsBlock2 = new TsBlock(timeColumn2, column2);
        mockTsBlocks.add(tsBlock);
        mockTsBlocks.add(tsBlock2);
        System.out.println("tsBlock need to send:");
        Column[] valueColumns = tsBlock.getValueColumns();
        System.out.println(" columns:");
        for(Column valueColumn:valueColumns){
            System.out.println(valueColumn);
        }
        TimeColumn timeColumn_send=tsBlock.getTimeColumn();
        long[] times=timeColumn_send.getTimes();
        System.out.println(" time columns:");
        for(long time:times){
            System.out.println(time);
        }
        Column[] valueColumns2 = tsBlock2.getValueColumns();
        System.out.println(" columns2:");
        for(Column valueColumn:valueColumns2){
            System.out.println(valueColumn);
        }
        TimeColumn timeColumn_send2=tsBlock2.getTimeColumn();
        long[] times2=timeColumn_send2.getTimes();
        System.out.println(" time columns2:");
        for(long time:times2){
            System.out.println(time);
        }

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        sinkChannel.send(mockTsBlocks.get(0));
        System.out.println("send successfully");
        int numOfMockTsBlock = 2;
        for (int i = 0; i < numOfMockTsBlock; i++) {
            try {
                sinkChannel.getSerializedTsBlock(i);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("getSerializedTsBlock successfully");
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        sinkChannel.acknowledgeTsBlock(0, numOfMockTsBlock);
        System.out.println("acknowledgeTsBlock successfully");

    }
}
