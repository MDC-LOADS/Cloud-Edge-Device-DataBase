package org.apache.iotdb.db.queryengine.plan.execution;


import org.apache.iotdb.db.zcy.service.CtoEService;
import org.apache.iotdb.db.zcy.service.TSInfo;
public class ServiceImpl implements CtoEService.Iface{
    private TSInfo message;

    public ServiceImpl() {
        this.message = new TSInfo(0, 0, 0, 0);
    }
    @Override
    public void sendData(TSInfo data) {
        // 实现 sendData 方法的逻辑
        try {
            System.out.println("Sending data: " + data);
            message.setSize(data.getSize());
            message.setNum(data.getNum());
            message.setMin(data.getMin());
            message.setMax(data.getMax());
        } catch (Exception e) {
            System.err.println("Error sending data: " + e.getMessage());
        }

    }
    @Override
    public TSInfo receiveData() {
        // 实现 receiveData 方法的逻辑
        try {
            System.out.println("Received data: " + message);
            return message;
        } catch (Exception e) {
            System.err.println("Error receiving data: " + e.getMessage());
            return null;
        }
    }
}
