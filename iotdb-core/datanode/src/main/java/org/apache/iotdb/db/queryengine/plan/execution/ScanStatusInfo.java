package org.apache.iotdb.db.queryengine.plan.execution;

import org.apache.iotdb.db.queryengine.execution.exchange.sink.ISinkHandle;
import org.apache.iotdb.db.queryengine.execution.exchange.sink.SinkChannel;

public class ScanStatusInfo{
    private int sourceId=0;
    private int edgeFragmentId=0;
    private int cloudFragmentId=0;
    private boolean status=false;//是否传输数据
    private int offset=0;//算子偏移量
    private ISinkHandle sinkHandle;

    ScanStatusInfo(int sourceId,int cloudFragmentId){
        this.cloudFragmentId=cloudFragmentId;
        this.sourceId=sourceId;
    }

    public void setEdgeFragmentId(int edgeFragmentId) {
        this.edgeFragmentId = edgeFragmentId;
    }

    public void setCloudFragmentId(int cloudFragmentId) {
        this.cloudFragmentId = cloudFragmentId;
    }

    public void setStatus(boolean status) {
        this.status = status;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getEdgeFragmentId() {
        return edgeFragmentId;
    }

    public int getCloudFragmentId() {
        return cloudFragmentId;
    }

    public boolean getStatus() {
        return status;
    }

    public int getOffset() {
        return offset;
    }

    public ISinkHandle getSinkHandle() {
        return sinkHandle;
    }

    public void setSinkHandle(ISinkHandle sinkHandle) {
        this.sinkHandle = sinkHandle;
    }
}
