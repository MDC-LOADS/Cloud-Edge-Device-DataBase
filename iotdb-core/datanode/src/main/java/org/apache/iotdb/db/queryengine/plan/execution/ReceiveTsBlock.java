package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.exchange.source.ISourceHandle;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.mpp.rpc.thrift.TFragmentInstanceId;
import org.apache.iotdb.tsfile.read.common.block.TsBlock;
import org.apache.iotdb.tsfile.read.common.block.column.Column;
import org.apache.iotdb.tsfile.read.common.block.column.TimeColumn;
import org.checkerframework.checker.units.qual.C;

import java.util.ArrayList;
import java.util.List;

public class ReceiveTsBlock {
    private static final MPPDataExchangeManager MPP_DATA_EXCHANGE_MANAGER =
            MPPDataExchangeService.getInstance().getMPPDataExchangeManager();
    public void receive(){
        final String queryId = "test_query";
        final TEndPoint remoteEndpoint = new TEndPoint("localhost", 10740);
        final TFragmentInstanceId remoteFragmentInstanceId = new TFragmentInstanceId(queryId, 0, "0");
        final String localPlanNodeId = "receive_test";
        final TFragmentInstanceId localFragmentInstanceId = new TFragmentInstanceId(queryId, 1, "0");
        long query_num=1;
        FragmentInstanceContext instanceContext = new FragmentInstanceContext(query_num);
        ISourceHandle sourceHandle =MPP_DATA_EXCHANGE_MANAGER.createSourceHandle(
                localFragmentInstanceId,
                localPlanNodeId,
                0,//IndexOfUpstreamSinkHandle
                remoteEndpoint,
                remoteFragmentInstanceId,
                instanceContext::failed);
        try {
            Thread.sleep(15000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        List<TsBlock> tsBlocks = new ArrayList<>();
        if(!sourceHandle.isFinished()){
            TsBlock tsBlock=sourceHandle.receive();
            tsBlocks.add(tsBlock);
        }
        sourceHandle.close();
        for (TsBlock tsBlock : tsBlocks){
            Column[] valueColumns = tsBlock.getValueColumns();
            System.out.println("receive columns:");
            for(Column valueColumn:valueColumns){
                System.out.println(valueColumn);
            }
            TimeColumn timeColumn=tsBlock.getTimeColumn();
            long[] times=timeColumn.getTimes();
            System.out.println("receive time columns:");
            for(long time:times){
                System.out.println(time);
            }
        }

    }
}
