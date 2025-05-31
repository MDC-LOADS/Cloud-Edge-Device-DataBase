package org.apache.iotdb.db.queryengine.plan.execution;



import org.apache.iotdb.db.zcy.service.PipeEtoCService;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

public class ServiceImpl implements PipeEtoCService.Iface{


    @Override
    public void AckMessage(int CloudFragmentId, int SourceId) throws TException {
        PipeInfo pipeInfo=PipeInfo.getInstance();
        pipeInfo.getScanStatus(SourceId).setCloudFragmentId(CloudFragmentId);
//        System.out.println("CloudFragmentId:"+CloudFragmentId+"sourceid"+SourceId);
        while((!pipeInfo.getScanStatus(SourceId).isSetOffset()) && (!pipeInfo.getScanStatus(SourceId).isSetStartTime())){
            try {
                Thread.sleep(10);//时间
//                System.out.println("waiting"+pipeInfo.getScanStatus(SourceId).isSetStartTime());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        //多线程非阻塞版本
        TTransport transport = null;
        try  {
            transport =  new TFramedTransport(new TSocket("localhost", 9091));
            TProtocol protocol = new TBinaryProtocol(transport);
            PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.AnsMessage(pipeInfo.getScanStatus(SourceId).getEdgeFragmentId(),SourceId,pipeInfo.getScanStatus(SourceId).getOffset());
//            System.out.println("ansData:"+SourceId+" sent successfully.");
        } catch (TException x) {
            x.printStackTrace();
        }finally {
            if(null!=transport){
                transport.close();
            }
        }
    }

}
