package zyh.service;


import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import oshi.SystemInfo;
import oshi.hardware.HWDiskStore;
import oshi.hardware.HWPartition;
import oshi.hardware.NetworkIF;
import java.io.File;


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

    private static double estimateDataTransferTime(double dataSize, double bandwidth) {
        // 根据数据大小和带宽估计传输时间
        return dataSize / bandwidth;
    }

    public static double getnetworkBandwidth() {
        SystemInfo systemInfo = new SystemInfo();
        NetworkIF[] networkIFs = systemInfo.getHardware().getNetworkIFs().toArray(new NetworkIF[0]);

        double sentBytesPerSecond = 0;
        double receivedBytesPerSecond = 0;
        for (NetworkIF networkIF : networkIFs) {

            // 获取先前的字节数
            long prevBytesSent = networkIF.getBytesSent();
            long prevBytesRecv = networkIF.getBytesRecv();

            // 更新网络接口信息
            networkIF.updateAttributes();

            // 获取当前的字节数
            long currentBytesSent = networkIF.getBytesSent();
            long currentBytesRecv = networkIF.getBytesRecv();

            // 计算网络带宽（假设每毫秒获取一次）
            double elapsedSeconds = (System.currentTimeMillis() - networkIF.getTimeStamp()) / 1.0;
            sentBytesPerSecond = (currentBytesSent - prevBytesSent) / elapsedSeconds;
            receivedBytesPerSecond = (currentBytesRecv - prevBytesRecv) / elapsedSeconds;

            System.out.println("Sent Bytes per Second: " + sentBytesPerSecond + " bytes/s");
            System.out.println("Received Bytes per Second: " + receivedBytesPerSecond + " bytes/s");

            // 更新上一次的字节数和时间戳
//            networkIF.setPrevBytesSent(networkIF.getBytesSent());
//            networkIF.setPrevBytesRecv(networkIF.getBytesRecv());
//            networkIF.setTimeStamp(System.currentTimeMillis());

        }
        return sentBytesPerSecond + receivedBytesPerSecond;
    }

    private static double getclouddatasize(int port, String address){

        double clouddatasize=0;
//        多线程非阻塞版本
//        TTransport transport = null;
//        try  {
//        transport =  new TFramedTransport(new TSocket(address, port));
//        TProtocol protocol = new TBinaryProtocol(transport);
//        CtoEService.Client client = new CtoEService.Client(protocol);
//        transport.open();
//        // 调用服务方法
//        TSInfo dataToReceive = client.receiveData();
//        System.out.println("Data receive successfully.");
//        //获取云端数据大小
//        clouddatasize = dataToReceive.getSize();
//        return clouddatasize;
//
//        } catch (TException x) {
//        x.printStackTrace();
//        }finally {
//        if(null!=transport){
//            transport.close();
//        }
//        }
//        System.out.println("Data receive failed.");
        return clouddatasize;

    }

    private static double getlocaldatasize(int port, String address){
            double fileSizeInMB = 0;
            double fileSizeInKB;

            // 定义目标文件路径
            String filePath = address;

            // 创建File对象
            File file = new File(filePath);

            // 检查文件是否存在
            if (file.exists()) {
                // 获取文件大小（以字节为单位）
                long fileSize = file.length();

                // 打印文件大小
                fileSizeInKB = fileSize / 1024.0;
                 fileSizeInMB = fileSizeInKB / 1024.0;

                System.out.println("File size: " + fileSizeInKB + " KB");
                System.out.println("File size: " + fileSizeInMB + " MB");
            } else {
                System.out.println("File does not exist.");
            }
        return fileSizeInMB;
    }


    public static double performLoadDetection(double localdataSize, double clouddataSize, double writeSpeed, double readspeed, double BytesPerSecond) {
        // 计算本地和云端的预计数据传输时间
        double flag=0;
        double localTime = estimateDataTransferTime(localdataSize, writeSpeed) +
                estimateDataTransferTime(localdataSize, readspeed);
        double cloudTime = estimateDataTransferTime(clouddataSize, BytesPerSecond) ;

        // 根据负载情况判断从本地还是云端获取数据
        if (localTime > cloudTime) {
            System.out.println("本地IO负载高，从云端获取数据。");
        } else {
            System.out.println("从本地获取数据。");
            flag=1;
        }
        return flag;
    }


    public static void main(String[] args) {
        int port=9090;
        String address="localhost";
        double readSpeed=getDiskReadSpeed();
        double writeSpeed=getDiskWriteSpeed();
        double BytesPerSecond=getnetworkBandwidth();
        double localdataSize=getlocaldatasize(port,address);
        double clouddataSize=getclouddatasize(port,address);
        double flag=performLoadDetection( localdataSize, clouddataSize,writeSpeed,readSpeed,BytesPerSecond);

        if (flag==0){
        //从云端计算数据
        }else{
        //从本地计算数据
        }

    }
}


