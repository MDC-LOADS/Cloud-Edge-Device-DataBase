package org.apache.iotdb.db.service;

import org.apache.iotdb.db.queryengine.plan.execution.PipeInfo;
import org.apache.iotdb.db.zcy.service.PipeCtoEService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
public class IOMonitor {
    private static final String DISK_STATS_FILE = "/proc/diskstats";
    private static final long MB = 1024 * 1024;

    private final long thresholdMBps;
    private int counter=0;
    private Map<String, DiskStats> previousStats = new HashMap<>();

    public IOMonitor(long thresholdMBps) {
        this.thresholdMBps = thresholdMBps;
    }

    public void monitor() throws IOException, InterruptedException {
        while (true) {
            Map<String, DiskStats> currentStats = readDiskStats();

            if (!previousStats.isEmpty()) {
                for (Map.Entry<String, DiskStats> entry : currentStats.entrySet()) {
                    String device = entry.getKey();
                    DiskStats current = entry.getValue();
                    DiskStats previous = previousStats.get(device);

                    if (previous != null) {
                        long readBytesDiff = current.readSectors * 512 - previous.readSectors * 512;
                        long writeBytesDiff = current.writeSectors * 512 - previous.writeSectors * 512;
                        long timeDiff = current.time - previous.time;

                        // Convert to MB/s
                        double readMBps = (double) readBytesDiff / MB ;
                        double writeMBps = (double) writeBytesDiff / MB ;

//                        System.out.printf("Device: %s, Read: %.2f MB/s, Write: %.2f MB/s%n",
//                                device, readMBps, writeMBps);

                        // Check threshold
                        if (readMBps + writeMBps > thresholdMBps) {
                            System.out.printf("IO Overload Alert! Device: %s, Total IO: %.2f MB/s (Threshold: %d MB/s)%n",
                                    device, readMBps + writeMBps, thresholdMBps);
//                            TimeUnit.SECONDS.sleep(5);
                            if(PipeInfo.getInstance().getSql()!=null){
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
                            }
                        }else{
                            counter++;
//                            System.out.println(counter);
                            if(counter>=40){
                                counter=0;
//                                System.out.println(counter);
                                if(PipeInfo.getInstance().getSql()!=null && PipeInfo.getInstance().getPipeStatus()){
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
                    }
                }
            }

            previousStats = currentStats;
            TimeUnit.SECONDS.sleep(1);
        }
    }

    private Map<String, DiskStats> readDiskStats() throws IOException {
        Map<String, DiskStats> stats = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(DISK_STATS_FILE))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.trim().split("\\s+");
                if (parts.length >= 14) {
                    String device = parts[2];
                    long readSectors = Long.parseLong(parts[5]);
                    long writeSectors = Long.parseLong(parts[9]);
                    long time = Long.parseLong(parts[12]); // time in milliseconds

                    stats.put(device, new DiskStats(readSectors, writeSectors, time));
                }
            }
        }

        return stats;
    }

    private static class DiskStats {
        final long readSectors;
        final long writeSectors;
        final long time;

        DiskStats(long readSectors, long writeSectors, long time) {
            this.readSectors = readSectors;
            this.writeSectors = writeSectors;
            this.time = time;
        }
    }
}
