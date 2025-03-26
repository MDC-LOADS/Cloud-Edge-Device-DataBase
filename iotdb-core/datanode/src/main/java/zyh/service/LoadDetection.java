package zyh.service;


import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HWPartition;

public class LoadDetection {
    private static double getDiskReadSpeed() {
        SystemInfo systemInfo = new SystemInfo();
        HWDiskStore[] diskStores = systemInfo.getHardware().getDiskStores().toArray(new HWDiskStore[0]);

        long readSpeed = 0;
        for (HWDiskStore diskStore : diskStores) {
            System.out.println("Disk: " + diskStore.getName());

            for (HWPartition partition : diskStore.getPartitions()) {
                System.out.println("Partition: " + partition.getMountPoint());

                // 获取磁盘读写速度（总字节数）
                long readBytes = diskStore.getReadBytes();

                // 休眠一段时间——1秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // 再次获取磁盘读写速度
                long newReadBytes = diskStore.getReadBytes();

                // 计算速度
                readSpeed = newReadBytes - readBytes;

            }
        }
        return readSpeed;
    }

    private static double getDiskWriteSpeed() {
        SystemInfo systemInfo = new SystemInfo();
        HWDiskStore[] diskStores = systemInfo.getHardware().getDiskStores().toArray(new HWDiskStore[0]);

        long writeSpeed = 0;
        for (HWDiskStore diskStore : diskStores) {
            System.out.println("Disk: " + diskStore.getName());

            for (HWPartition partition : diskStore.getPartitions()) {
                System.out.println("Partition: " + partition.getMountPoint());

                // 获取磁盘读写速度（总字节数）
                long writeBytes = diskStore.getWriteBytes();

                // 休眠一段时间，例如1秒
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                // 再次获取磁盘读写速度
                long newWriteBytes = diskStore.getWriteBytes();

                // 计算速度
                writeSpeed = newWriteBytes - writeBytes;
            }
        }
        return writeSpeed;
    }

    private static double estimateDataTransferTime(double Currentrate, double Maxrate) {
        // 根据当前读取速率和最大传输速率
        return Currentrate / Maxrate;
    }


    public static double performLoadDetection( double writeSpeed, double readspeed,double maxrate,double threshold) {
        // 计算本地和云端的预计数据速率
        double flag=0;
        double currentrate=writeSpeed+readspeed;
        double rateThreshold=estimateDataTransferTime(currentrate,maxrate);
        // 根据负载情况判断从本地还是云端获取数据
        if (rateThreshold < threshold) {
            System.out.println("本地IO负载高，从云端获取数据");
        } else {
            System.out.println("从本地获取数据");
            flag=1;
        }
        return flag;
    }

    public static void monitor() {
        double maxrate=2e10;
        double threshold=0.5;
        double readSpeed=getDiskReadSpeed();
        double writeSpeed=getDiskWriteSpeed();
        double flag=performLoadDetection( writeSpeed,readSpeed,maxrate,threshold);
        if(PipeInfo.getInstance().getSql()!=null){
            if (flag==0){
                //从云端计算数据
                PipeInfo.getInstance().setPipeStatus(true);
                TTransport transport = null;
                try  {
                    transport =  new TFramedTransport(new TSocket("localhost", 9091));
                    TProtocol protocol = new TBinaryProtocol(transport);
                    PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
                    transport.open();
                    // 调用服务方法
                    client.PipeStart(PipeInfo.getInstance().getSql());
                    System.out.println("start successfully.");

                } catch (TException x) {
                    x.printStackTrace();
                }finally {
                    if(null!=transport){
                        transport.close();
                    }
                }
                System.out.println("pipe start");
            }else{
                //从本地计算数据
                PipeInfo.getInstance().setPipeStatus(false);
                TTransport transport = null;
                try  {
                    transport =  new TFramedTransport(new TSocket("localhost", 9091));
                    TProtocol protocol = new TBinaryProtocol(transport);
                    PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
                    transport.open();
                    // 调用服务方法
                    client.PipeClose();
                    System.out.println("stop successfully.");

                } catch (TException x) {
                    x.printStackTrace();
                }finally {
                    if(null!=transport){
                        transport.close();
                    }
                }
                System.out.println("pipe stop");
            }
        }


    }
    public void PipeStop(){
        PipeInfo.getInstance().setPipeStatus(false);
        TTransport transport = null;
        try  {
            transport =  new TFramedTransport(new TSocket("localhost", 9091));
            TProtocol protocol = new TBinaryProtocol(transport);
            PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.PipeClose();
            System.out.println("stop successfully.");

        } catch (TException x) {
            x.printStackTrace();
        }finally {
            if(null!=transport){
                transport.close();
            }
        }
        System.out.println("pipe stop");
    }
    public void PipeStart(){
        PipeInfo.getInstance().setPipeStatus(true);
        TTransport transport = null;
        try  {
            transport =  new TFramedTransport(new TSocket("localhost", 9091));
            TProtocol protocol = new TBinaryProtocol(transport);
            PipeCtoEService.Client client = new PipeCtoEService.Client(protocol);
            transport.open();
            // 调用服务方法
            client.PipeStart(PipeInfo.getInstance().getSql());
            System.out.println("start successfully.");

        } catch (TException x) {
            x.printStackTrace();
        }finally {
            if(null!=transport){
                transport.close();
            }
        }
        System.out.println("pipe start");
    }
}





