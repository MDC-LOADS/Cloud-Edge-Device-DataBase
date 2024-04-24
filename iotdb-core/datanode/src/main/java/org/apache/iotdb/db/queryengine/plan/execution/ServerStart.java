package org.apache.iotdb.db.queryengine.plan.execution;


import org.apache.iotdb.db.zcy.service.PipeEtoCService;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TNonblockingServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;


public class ServerStart {
    public void start(){
        //多线程阻塞


//        try {
//            TServerTransport serverTransport = new TServerSocket(9090);
//            // 创建 Thrift 服务器端
//            ServiceImpl handler = new ServiceImpl();
//            CtoEService.Processor processor = new CtoEService.Processor(handler);
//
//            TBinaryProtocol.Factory protocolFactory=new TBinaryProtocol.Factory();
//            TThreadPoolServer.Args targs=new TThreadPoolServer.Args(serverTransport);
//            targs.processor(processor);
//            targs.protocolFactory(protocolFactory);
//            TServer server = new TThreadPoolServer(targs);
//            // 启动服务器
//            if(!server.isServing()){
//                System.out.println("Starting the receiver server...");
//                server.serve();
//            }
//        } catch (TTransportException e) {
//            e.printStackTrace();
//        }
        //多线程非阻塞
        try{
            TNonblockingServerSocket transport =new TNonblockingServerSocket(9090);
            PipeEtoCService.Processor processor = new PipeEtoCService.Processor(new ServiceImpl());
            TBinaryProtocol.Factory protocolFactory = new TBinaryProtocol.Factory();
            TFramedTransport.Factory tTransport = new TFramedTransport.Factory();

            THsHaServer.Args targs = new THsHaServer.Args(transport);
            targs.processor(processor);
            targs.protocolFactory(protocolFactory);
            targs.transportFactory(tTransport);

            TServer server = new THsHaServer(targs);
            System.out.println("Starting the edge server...");
            server.serve();
        }catch(Exception e){
            e.printStackTrace();
        }

    }
}
