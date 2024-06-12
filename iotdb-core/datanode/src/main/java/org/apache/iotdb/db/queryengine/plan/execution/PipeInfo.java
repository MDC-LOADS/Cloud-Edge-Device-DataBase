package org.apache.iotdb.db.queryengine.plan.execution;

import java.util.Map;
import java.util.HashMap;
public class PipeInfo {
    private static final PipeInfo instance=new PipeInfo();

    // 声明单例对象需要修改的属性
    private boolean pipeStatus;//pipe的启动状态 0：关闭  1：启动
    private Map<Integer,ScanStatusInfo> scanStatusInfos;
    private int fragmentId;
    private String sql;


    // 私有构造方法，避免外部实例化
    private PipeInfo() {
        this.pipeStatus=false;
        this.scanStatusInfos=new HashMap<>();
        this.fragmentId=1000;
    }

    // 提供获取实例的静态方法，使用 synchronized 关键字保证线程安全
    public static synchronized PipeInfo getInstance() {
        return instance;
    }

    // 设置单例对象的值
    public void setPipeStatus(boolean status){
        this.pipeStatus=status;
    }
    public void addScanSatus(int sourceId, int edgeFragmentId) {//添加算子
        ScanStatusInfo scanStatusInfo = new ScanStatusInfo(sourceId, edgeFragmentId);
        scanStatusInfos.put(sourceId, scanStatusInfo);
    }

    // 获取单例对象的值
    public boolean getPipeStatus(){
        return  pipeStatus;
    }
    public ScanStatusInfo getScanStatus(int sourceId){
        return scanStatusInfos.get(sourceId);
    }
    public void printAllScanStatus() {
        for (Map.Entry<Integer, ScanStatusInfo> entry : scanStatusInfos.entrySet()) {
            int id = entry.getKey();
            ScanStatusInfo scanStatusInfo = entry.getValue();
            System.out.println("Source ID: " + id + ", Edge Fragment: " + scanStatusInfo.getEdgeFragmentId() );
        }
    }

    public int getFragmentId() {
        return fragmentId++;
    }

    public void clearAllScanStatus(){
//        this.scanStatusInfos=new HashMap<>();
        sql=null;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
