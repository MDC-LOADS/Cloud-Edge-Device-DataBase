package org.apache.iotdb.db.queryengine.plan.execution;


import org.apache.iotdb.db.zcy.service.PipeCtoEService;
import org.apache.iotdb.db.zcy.service.ScanInfo;
import org.apache.thrift.TException;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;

import java.util.HashMap;

public class ServiceImpl implements PipeCtoEService.Iface{

    @Override
    public void PipeStart(String sql) throws TException {
        Thread thread = new Thread(new ExcuteSqlRunnable(sql));//发送数据测试
        thread.start();
        PipeInfo pipeInfo = PipeInfo.getInstance();//设置pipe状态为启动
        pipeInfo.setPipeStatus(true);

    }

    @Override
    public void AnsMessage(int EdgeFragmentId, int SourceId, int ReadOffset) throws TException {
        PipeInfo pipeInfo=PipeInfo.getInstance();
        pipeInfo.getScanStatus(SourceId).setEdgeFragmentId(EdgeFragmentId);
        pipeInfo.getScanStatus(SourceId).setOffset(ReadOffset);
        pipeInfo.getScanStatus(SourceId).setStatus(true);
    }

    @Override
    public ScanInfo PipeClose() throws TException {
        PipeInfo pipeInfo=PipeInfo.getInstance();
        pipeInfo.closeAllScanStatus();
        pipeInfo.setPipeStatus(false);
        pipeInfo.clearAllScanStatus();
        return null;
    }

}
class ExcuteSqlRunnable implements Runnable {
    private final String sql;
    public ExcuteSqlRunnable(String sql){
        this.sql=sql;
    }
    @Override
    public void run() {

        ClientRPCServiceImpl clientRPCService = new ClientRPCServiceImpl();
        clientRPCService.excuteIdentitySql(sql);
    }
}
