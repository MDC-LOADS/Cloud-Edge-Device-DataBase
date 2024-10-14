package org.apache.iotdb.db.queryengine.plan.execution;

import java.util.Map;
import java.util.HashMap;

public class PipeInfo {
    private static final PipeInfo instance=new PipeInfo();

    // 声明单例对象需要修改的属性
    private boolean pipeStatus;//pipe的启动状态 0：关闭  1：启动
    private Map<Integer,ScanStatusInfo> scanStatusInfos;
    private int fragmentId;

    private boolean aggregationStatus;


    // 私有构造方法，避免外部实例化
    private PipeInfo() {
        this.pipeStatus=false;
        this.scanStatusInfos=new HashMap<>();
        this.fragmentId=1000;
        this.aggregationStatus=false;
    }

    // 提供获取实例的静态方法，使用 synchronized 关键字保证线程安全
    public static synchronized PipeInfo getInstance() {
        return instance;
    }

    // 设置单例对象的值
    public void setPipeStatus(boolean status){
        this.pipeStatus=status;
    }
    public void addScanSatus(int sourceId, int cloudFragmentId) {//添加算子
        ScanStatusInfo scanStatusInfo = new ScanStatusInfo(sourceId, cloudFragmentId);
        scanStatusInfos.put(sourceId, scanStatusInfo);
    }
    public void setAggregationStatus(boolean status){
        this.aggregationStatus=status;
    }

    // 获取单例对象的值
    public boolean getPipeStatus(){
        return  pipeStatus;
    }
    public boolean getAggregationStatus(){
        return aggregationStatus;
    }

    public ScanStatusInfo getScanStatus(int sourceId){
        return scanStatusInfos.get(sourceId);
    }
    public int getFragmentId() {
        return fragmentId++;
    }
    public void closeAllScanStatus() {
        for (Map.Entry<Integer, ScanStatusInfo> entry : scanStatusInfos.entrySet()) {
            ScanStatusInfo scanStatusInfo = entry.getValue();
            scanStatusInfo.getSinkHandle().setNoMoreTsBlocksOfOneChannel(0);
            scanStatusInfo.getSinkHandle().close();//可能不需要
            scanStatusInfo.setStatus(false);
        }
    }
    public void clearAllScanStatus(){
        this.scanStatusInfos=new HashMap<>();
    }
}
