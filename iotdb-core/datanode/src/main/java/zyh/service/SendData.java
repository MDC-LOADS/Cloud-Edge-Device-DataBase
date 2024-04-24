package zyh.service;



import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class SendData{
    public void senddata(){
        //多线程非阻塞版本
//        TTransport transport = null;
//        try  {
//        transport =  new TFramedTransport(new TSocket("localhost", 9090));
//        TProtocol protocol = new TBinaryProtocol(transport);
//        CtoEService.Client client = new CtoEService.Client(protocol);
//        transport.open();
//        // 调用服务方法
//        TSInfo dataToSend = new TSInfo(11, 12, 13, 14);
//        client.sendData(dataToSend);
//        System.out.println("Data sent successfully.");
//
//        } catch (TException x) {
//            x.printStackTrace();
//        }finally {
//            if(null!=transport){
//                transport.close();
//            }
//        }
    }

}
