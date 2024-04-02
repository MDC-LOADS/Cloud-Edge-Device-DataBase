package zyh.service;

import org.apache.iotdb.db.zcy.service.CtoEService;
import org.apache.iotdb.db.zcy.service.TSInfo;

import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.TException;
import org.apache.thrift.transport.layered.TFramedTransport;

public class SendData{
    public void senddata(){
        TTransport transport=null;
        try
        {
            transport = new TframedTransport(new TSocket(host:"localhost", port:9090));
            TProtocol protocol = new TBinaryProtocol(transport);
            CtoEService.Client client = new CtoEService.Client(protocol);
            transport.open();
            //method of service
            TSInfo dataTosend = new TSInfo(size:11, num:12, min:13, max:14);
            client.sendData(dataTosend);
            System.out.println("Data sent successfully");
            TSInfo receivedata = client.receiveData();
            System.out.println(receivedata);
        }catch(TException x){
            x.printStackTrace();
        }finally{
            if(null!=transport) {
                transport.close();
            }
        }
    }

}
