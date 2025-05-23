/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.confignode.persistence.executor;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.common.rpc.thrift.TSchemaNode;
import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.MNodeType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;
import org.apache.iotdb.confignode.consensus.request.ConfigPhysicalPlan;
import org.apache.iotdb.confignode.consensus.request.auth.AuthorPlan;
import org.apache.iotdb.confignode.consensus.request.read.database.CountDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.database.GetDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.read.datanode.GetDataNodeConfigurationPlan;
import org.apache.iotdb.confignode.consensus.request.read.function.GetUDFJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.CountTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetNodePathsPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetSeriesSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.partition.GetTimeSlotListPlan;
import org.apache.iotdb.confignode.consensus.request.read.pipe.plugin.GetPipePluginJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionIdPlan;
import org.apache.iotdb.confignode.consensus.request.read.region.GetRegionInfoListPlan;
import org.apache.iotdb.confignode.consensus.request.read.template.CheckTemplateSettablePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetPathsSetTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.read.template.GetTemplateSetInfoPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerJarPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.read.trigger.GetTriggerTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.ApplyConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.RemoveConfigNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.confignode.UpdateVersionInfoPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.ActiveCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.AddCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.DropCQPlan;
import org.apache.iotdb.confignode.consensus.request.write.cq.UpdateCQLastExecTimePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.AdjustMaxRegionGroupNumPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DatabaseSchemaPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.DeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.PreDeleteDatabasePlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetDataReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetSchemaReplicationFactorPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTTLPlan;
import org.apache.iotdb.confignode.consensus.request.write.database.SetTimePartitionIntervalPlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RegisterDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.RemoveDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.datanode.UpdateDataNodePlan;
import org.apache.iotdb.confignode.consensus.request.write.function.CreateFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.function.DropFunctionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateDataPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.CreateSchemaPartitionPlan;
import org.apache.iotdb.confignode.consensus.request.write.partition.UpdateRegionLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.CreatePipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.plugin.DropPipePluginPlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleLeaderChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.runtime.PipeHandleMetaChangePlan;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.CreatePipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.DropPipePlanV2;
import org.apache.iotdb.confignode.consensus.request.write.pipe.task.SetPipeStatusPlanV2;
import org.apache.iotdb.confignode.consensus.request.write.procedure.DeleteProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.procedure.UpdateProcedurePlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetSpaceQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.quota.SetThrottleQuotaPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.CreateRegionGroupsPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.OfferRegionMaintainTasksPlan;
import org.apache.iotdb.confignode.consensus.request.write.region.PollSpecificRegionMaintainTaskPlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CommitSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.CreateSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.DropSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.ExtendSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreSetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.PreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.RollbackPreUnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.SetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.template.UnsetSchemaTemplatePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.AddTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.DeleteTriggerInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerLocationPlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggerStateInTablePlan;
import org.apache.iotdb.confignode.consensus.request.write.trigger.UpdateTriggersOnTransferNodesPlan;
import org.apache.iotdb.confignode.consensus.response.partition.SchemaNodeManagementResp;
import org.apache.iotdb.confignode.exception.physical.UnknownPhysicalPlanTypeException;
import org.apache.iotdb.confignode.persistence.AuthorInfo;
import org.apache.iotdb.confignode.persistence.ProcedureInfo;
import org.apache.iotdb.confignode.persistence.TriggerInfo;
import org.apache.iotdb.confignode.persistence.UDFInfo;
import org.apache.iotdb.confignode.persistence.cq.CQInfo;
import org.apache.iotdb.confignode.persistence.node.NodeInfo;
import org.apache.iotdb.confignode.persistence.partition.PartitionInfo;
import org.apache.iotdb.confignode.persistence.pipe.PipeInfo;
import org.apache.iotdb.confignode.persistence.quota.QuotaInfo;
import org.apache.iotdb.confignode.persistence.schema.ClusterSchemaInfo;
import org.apache.iotdb.confignode.rpc.thrift.TDatabaseSchema;
import org.apache.iotdb.confignode.rpc.thrift.TShowRegionReq;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.tsfile.utils.Pair;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class ConfigPlanExecutor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigPlanExecutor.class);

  /**
   * Every Info class that implement SnapshotProcessor in ConfigNode should be registered in
   * snapshotProcessorList.
   */
  private final List<SnapshotProcessor> snapshotProcessorList;

  private final NodeInfo nodeInfo;

  private final ClusterSchemaInfo clusterSchemaInfo;

  private final PartitionInfo partitionInfo;

  private final AuthorInfo authorInfo;

  private final ProcedureInfo procedureInfo;

  private final UDFInfo udfInfo;

  private final TriggerInfo triggerInfo;

  private final CQInfo cqInfo;

  private final PipeInfo pipeInfo;

  private final QuotaInfo quotaInfo;

  public ConfigPlanExecutor(
      NodeInfo nodeInfo,
      ClusterSchemaInfo clusterSchemaInfo,
      PartitionInfo partitionInfo,
      AuthorInfo authorInfo,
      ProcedureInfo procedureInfo,
      UDFInfo udfInfo,
      TriggerInfo triggerInfo,
      CQInfo cqInfo,
      PipeInfo pipeInfo,
      QuotaInfo quotaInfo) {

    this.snapshotProcessorList = new ArrayList<>();

    this.nodeInfo = nodeInfo;
    this.snapshotProcessorList.add(nodeInfo);

    this.clusterSchemaInfo = clusterSchemaInfo;
    this.snapshotProcessorList.add(clusterSchemaInfo);

    this.partitionInfo = partitionInfo;
    this.snapshotProcessorList.add(partitionInfo);

    this.authorInfo = authorInfo;
    this.snapshotProcessorList.add(authorInfo);

    this.triggerInfo = triggerInfo;
    this.snapshotProcessorList.add(triggerInfo);

    this.udfInfo = udfInfo;
    this.snapshotProcessorList.add(udfInfo);

    this.cqInfo = cqInfo;
    this.snapshotProcessorList.add(cqInfo);

    this.pipeInfo = pipeInfo;
    this.snapshotProcessorList.add(pipeInfo);

    this.procedureInfo = procedureInfo;

    this.quotaInfo = quotaInfo;
    this.snapshotProcessorList.add(quotaInfo);
  }

  public DataSet executeQueryPlan(ConfigPhysicalPlan req)
      throws UnknownPhysicalPlanTypeException, AuthException {
    switch (req.getType()) {
      case GetDataNodeConfiguration:
        return nodeInfo.getDataNodeConfiguration((GetDataNodeConfigurationPlan) req);
      case CountDatabase:
        return clusterSchemaInfo.countMatchedDatabases((CountDatabasePlan) req);
      case GetDatabase:
        return clusterSchemaInfo.getMatchedDatabaseSchemas((GetDatabasePlan) req);
      case GetDataPartition:
      case GetOrCreateDataPartition:
        return partitionInfo.getDataPartition((GetDataPartitionPlan) req);
      case GetSchemaPartition:
      case GetOrCreateSchemaPartition:
        return partitionInfo.getSchemaPartition((GetSchemaPartitionPlan) req);
      case ListUser:
        return authorInfo.executeListUsers((AuthorPlan) req);
      case ListRole:
        return authorInfo.executeListRoles((AuthorPlan) req);
      case ListUserPrivilege:
        return authorInfo.executeListUserPrivileges((AuthorPlan) req);
      case ListRolePrivilege:
        return authorInfo.executeListRolePrivileges((AuthorPlan) req);
      case GetNodePathsPartition:
        return getSchemaNodeManagementPartition(req);
      case GetRegionInfoList:
        return getRegionInfoList(req);
      case GetAllSchemaTemplate:
        return clusterSchemaInfo.getAllTemplates();
      case GetSchemaTemplate:
        return clusterSchemaInfo.getTemplate((GetSchemaTemplatePlan) req);
      case CheckTemplateSettable:
        return clusterSchemaInfo.checkTemplateSettable((CheckTemplateSettablePlan) req);
      case GetPathsSetTemplate:
        return clusterSchemaInfo.getPathsSetTemplate((GetPathsSetTemplatePlan) req);
      case GetAllTemplateSetInfo:
        return clusterSchemaInfo.getAllTemplateSetInfo();
      case GetTemplateSetInfo:
        return clusterSchemaInfo.getTemplateSetInfo((GetTemplateSetInfoPlan) req);
      case GetTriggerTable:
        return triggerInfo.getTriggerTable((GetTriggerTablePlan) req);
      case GetTriggerLocation:
        return triggerInfo.getTriggerLocation((GetTriggerLocationPlan) req);
      case GetTriggerJar:
        return triggerInfo.getTriggerJar((GetTriggerJarPlan) req);
      case GetTransferringTriggers:
        return triggerInfo.getTransferringTriggers();
      case GetRegionId:
        return partitionInfo.getRegionId((GetRegionIdPlan) req);
      case GetTimeSlotList:
        return partitionInfo.getTimeSlotList((GetTimeSlotListPlan) req);
      case CountTimeSlotList:
        return partitionInfo.countTimeSlotList((CountTimeSlotListPlan) req);
      case GetSeriesSlotList:
        return partitionInfo.getSeriesSlotList((GetSeriesSlotListPlan) req);
      case SHOW_CQ:
        return cqInfo.showCQ();
      case GetFunctionTable:
        return udfInfo.getUDFTable();
      case GetFunctionJar:
        return udfInfo.getUDFJar((GetUDFJarPlan) req);
      case GetPipePluginTable:
        return pipeInfo.getPipePluginInfo().showPipePlugins();
      case GetPipePluginJar:
        return pipeInfo.getPipePluginInfo().getPipePluginJar((GetPipePluginJarPlan) req);
      case ShowPipeV2:
        return pipeInfo.getPipeTaskInfo().showPipes();
      default:
        throw new UnknownPhysicalPlanTypeException(req.getType());
    }
  }

  public TSStatus executeNonQueryPlan(ConfigPhysicalPlan physicalPlan)
      throws UnknownPhysicalPlanTypeException {
    switch (physicalPlan.getType()) {
      case RegisterDataNode:
        return nodeInfo.registerDataNode((RegisterDataNodePlan) physicalPlan);
      case RemoveDataNode:
        return nodeInfo.removeDataNode((RemoveDataNodePlan) physicalPlan);
      case UpdateDataNodeConfiguration:
        return nodeInfo.updateDataNode((UpdateDataNodePlan) physicalPlan);
      case CreateDatabase:
        TSStatus status = clusterSchemaInfo.createDatabase((DatabaseSchemaPlan) physicalPlan);
        if (status.getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
          return status;
        }
        return partitionInfo.createDatabase((DatabaseSchemaPlan) physicalPlan);
      case AlterDatabase:
        return clusterSchemaInfo.alterDatabase((DatabaseSchemaPlan) physicalPlan);
      case AdjustMaxRegionGroupNum:
        return clusterSchemaInfo.adjustMaxRegionGroupCount(
            (AdjustMaxRegionGroupNumPlan) physicalPlan);
      case DeleteDatabase:
        try {
          return clusterSchemaInfo.deleteDatabase((DeleteDatabasePlan) physicalPlan);
        } finally {
          partitionInfo.deleteDatabase((DeleteDatabasePlan) physicalPlan);
        }
      case PreDeleteDatabase:
        return partitionInfo.preDeleteDatabase((PreDeleteDatabasePlan) physicalPlan);
      case SetTTL:
        return clusterSchemaInfo.setTTL((SetTTLPlan) physicalPlan);
      case SetSchemaReplicationFactor:
        return clusterSchemaInfo.setSchemaReplicationFactor(
            (SetSchemaReplicationFactorPlan) physicalPlan);
      case SetDataReplicationFactor:
        return clusterSchemaInfo.setDataReplicationFactor(
            (SetDataReplicationFactorPlan) physicalPlan);
      case SetTimePartitionInterval:
        return clusterSchemaInfo.setTimePartitionInterval(
            (SetTimePartitionIntervalPlan) physicalPlan);
      case CreateRegionGroups:
        return partitionInfo.createRegionGroups((CreateRegionGroupsPlan) physicalPlan);
      case OfferRegionMaintainTasks:
        return partitionInfo.offerRegionMaintainTasks((OfferRegionMaintainTasksPlan) physicalPlan);
      case PollRegionMaintainTask:
        return partitionInfo.pollRegionMaintainTask();
      case PollSpecificRegionMaintainTask:
        return partitionInfo.pollSpecificRegionMaintainTask(
            (PollSpecificRegionMaintainTaskPlan) physicalPlan);
      case CreateSchemaPartition:
        return partitionInfo.createSchemaPartition((CreateSchemaPartitionPlan) physicalPlan);
      case CreateDataPartition:
        return partitionInfo.createDataPartition((CreateDataPartitionPlan) physicalPlan);
      case UpdateProcedure:
        return procedureInfo.updateProcedure((UpdateProcedurePlan) physicalPlan);
      case DeleteProcedure:
        return procedureInfo.deleteProcedure((DeleteProcedurePlan) physicalPlan);
      case CreateUser:
      case CreateRole:
      case DropUser:
      case DropRole:
      case GrantRole:
      case GrantUser:
      case GrantRoleToUser:
      case RevokeUser:
      case RevokeRole:
      case RevokeRoleFromUser:
      case UpdateUser:
      case CreateUserDep:
      case CreateRoleDep:
      case DropUserDep:
      case DropRoleDep:
      case GrantRoleDep:
      case GrantUserDep:
      case GrantRoleToUserDep:
      case RevokeUserDep:
      case RevokeRoleDep:
      case RevokeRoleFromUserDep:
      case UpdateUserDep:
        return authorInfo.authorNonQuery((AuthorPlan) physicalPlan);
      case ApplyConfigNode:
        return nodeInfo.applyConfigNode((ApplyConfigNodePlan) physicalPlan);
      case RemoveConfigNode:
        return nodeInfo.removeConfigNode((RemoveConfigNodePlan) physicalPlan);
      case UpdateVersionInfo:
        return nodeInfo.updateVersionInfo((UpdateVersionInfoPlan) physicalPlan);
      case CreateFunction:
        return udfInfo.addUDFInTable((CreateFunctionPlan) physicalPlan);
      case DropFunction:
        return udfInfo.dropFunction((DropFunctionPlan) physicalPlan);
      case AddTriggerInTable:
        return triggerInfo.addTriggerInTable((AddTriggerInTablePlan) physicalPlan);
      case DeleteTriggerInTable:
        return triggerInfo.deleteTriggerInTable((DeleteTriggerInTablePlan) physicalPlan);
      case UpdateTriggerStateInTable:
        return triggerInfo.updateTriggerStateInTable((UpdateTriggerStateInTablePlan) physicalPlan);
      case UpdateTriggersOnTransferNodes:
        return triggerInfo.updateTriggersOnTransferNodes(
            (UpdateTriggersOnTransferNodesPlan) physicalPlan);
      case UpdateTriggerLocation:
        return triggerInfo.updateTriggerLocation((UpdateTriggerLocationPlan) physicalPlan);
      case CreateSchemaTemplate:
        return clusterSchemaInfo.createSchemaTemplate((CreateSchemaTemplatePlan) physicalPlan);
      case UpdateRegionLocation:
        return partitionInfo.updateRegionLocation((UpdateRegionLocationPlan) physicalPlan);
      case SetSchemaTemplate:
        return clusterSchemaInfo.setSchemaTemplate((SetSchemaTemplatePlan) physicalPlan);
      case PreSetSchemaTemplate:
        return clusterSchemaInfo.preSetSchemaTemplate((PreSetSchemaTemplatePlan) physicalPlan);
      case CommitSetSchemaTemplate:
        return clusterSchemaInfo.commitSetSchemaTemplate(
            (CommitSetSchemaTemplatePlan) physicalPlan);
      case PreUnsetTemplate:
        return clusterSchemaInfo.preUnsetSchemaTemplate((PreUnsetSchemaTemplatePlan) physicalPlan);
      case RollbackUnsetTemplate:
        return clusterSchemaInfo.rollbackUnsetSchemaTemplate(
            (RollbackPreUnsetSchemaTemplatePlan) physicalPlan);
      case UnsetTemplate:
        return clusterSchemaInfo.unsetSchemaTemplate((UnsetSchemaTemplatePlan) physicalPlan);
      case DropSchemaTemplate:
        return clusterSchemaInfo.dropSchemaTemplate((DropSchemaTemplatePlan) physicalPlan);
      case ExtendSchemaTemplate:
        return clusterSchemaInfo.extendSchemaTemplate((ExtendSchemaTemplatePlan) physicalPlan);
      case CreatePipeV2:
        return pipeInfo.getPipeTaskInfo().createPipe((CreatePipePlanV2) physicalPlan);
      case SetPipeStatusV2:
        return pipeInfo.getPipeTaskInfo().setPipeStatus((SetPipeStatusPlanV2) physicalPlan);
      case DropPipeV2:
        return pipeInfo.getPipeTaskInfo().dropPipe((DropPipePlanV2) physicalPlan);
      case PipeHandleLeaderChange:
        return pipeInfo
            .getPipeTaskInfo()
            .handleLeaderChange((PipeHandleLeaderChangePlan) physicalPlan);
      case PipeHandleMetaChange:
        return pipeInfo
            .getPipeTaskInfo()
            .handleMetaChanges((PipeHandleMetaChangePlan) physicalPlan);
      case ADD_CQ:
        return cqInfo.addCQ((AddCQPlan) physicalPlan);
      case DROP_CQ:
        return cqInfo.dropCQ((DropCQPlan) physicalPlan);
      case ACTIVE_CQ:
        return cqInfo.activeCQ((ActiveCQPlan) physicalPlan);
      case UPDATE_CQ_LAST_EXEC_TIME:
        return cqInfo.updateCQLastExecutionTime((UpdateCQLastExecTimePlan) physicalPlan);
      case CreatePipePlugin:
        return pipeInfo.getPipePluginInfo().createPipePlugin((CreatePipePluginPlan) physicalPlan);
      case DropPipePlugin:
        return pipeInfo.getPipePluginInfo().dropPipePlugin((DropPipePluginPlan) physicalPlan);
      case CreatePipeSinkV1:
      case DropPipeV1:
      case DropPipeSinkV1:
      case GetPipeSinkV1:
      case PreCreatePipeV1:
      case RecordPipeMessageV1:
      case SetPipeStatusV1:
      case ShowPipeV1:
        return new TSStatus(TSStatusCode.INCOMPATIBLE_VERSION.getStatusCode());
      case setSpaceQuota:
        return quotaInfo.setSpaceQuota((SetSpaceQuotaPlan) physicalPlan);
      case setThrottleQuota:
        return quotaInfo.setThrottleQuota((SetThrottleQuotaPlan) physicalPlan);
      default:
        throw new UnknownPhysicalPlanTypeException(physicalPlan.getType());
    }
  }

  public boolean takeSnapshot(File snapshotDir) {

    // consensus layer needs to ensure that the directory exists.
    // if it does not exist, print a log to warn there may have a problem.
    if (!snapshotDir.exists()) {
      LOGGER.warn(
          "snapshot directory [{}] is not exist,start to create it.",
          snapshotDir.getAbsolutePath());
      // try to create a directory to enable snapshot operation
      if (!snapshotDir.mkdirs()) {
        LOGGER.error("snapshot directory [{}] can not be created.", snapshotDir.getAbsolutePath());
        return false;
      }
    }

    // If the directory is not empty, we should not continue the snapshot operation,
    // which may result in incorrect results.
    File[] fileList = snapshotDir.listFiles();
    if (fileList != null && fileList.length > 0) {
      LOGGER.error("snapshot directory [{}] is not empty.", snapshotDir.getAbsolutePath());
      return false;
    }

    AtomicBoolean result = new AtomicBoolean(true);
    snapshotProcessorList.forEach(
        x -> {
          boolean takeSnapshotResult = true;
          try {
            long startTime = System.currentTimeMillis();
            LOGGER.info(
                "[ConfigNodeSnapshot] Start to take snapshot for {} into {}",
                x.getClass().getName(),
                snapshotDir.getAbsolutePath());
            takeSnapshotResult = x.processTakeSnapshot(snapshotDir);
            LOGGER.info(
                "[ConfigNodeSnapshot] Finish to take snapshot for {}, time consumption: {} ms",
                x.getClass().getName(),
                System.currentTimeMillis() - startTime);
          } catch (TException | IOException e) {
            LOGGER.error("Take snapshot error: {}", e.getMessage());
            takeSnapshotResult = false;
          } finally {
            // If any snapshot fails, the whole fails
            // So this is just going to be false
            if (!takeSnapshotResult) {
              result.set(false);
            }
          }
        });
    if (result.get()) {
      LOGGER.info("[ConfigNodeSnapshot] Task snapshot success, snapshotDir: {}", snapshotDir);
    }
    return result.get();
  }

  public void loadSnapshot(File latestSnapshotRootDir) {
    if (!latestSnapshotRootDir.exists()) {
      LOGGER.error(
          "snapshot directory [{}] is not exist, can not load snapshot with this directory.",
          latestSnapshotRootDir.getAbsolutePath());
      return;
    }

    AtomicBoolean result = new AtomicBoolean(true);
    snapshotProcessorList
        .parallelStream()
        .forEach(
            x -> {
              try {
                long startTime = System.currentTimeMillis();
                LOGGER.info(
                    "[ConfigNodeSnapshot] Start to load snapshot for {} from {}",
                    x.getClass().getName(),
                    latestSnapshotRootDir.getAbsolutePath());
                x.processLoadSnapshot(latestSnapshotRootDir);
                LOGGER.info(
                    "[ConfigNodeSnapshot] Load snapshot for {} cost {} ms",
                    x.getClass().getName(),
                    System.currentTimeMillis() - startTime);
              } catch (TException | IOException e) {
                result.set(false);
                LOGGER.error("Load snapshot error: {}", e.getMessage());
              }
            });
    if (result.get()) {
      LOGGER.info(
          "[ConfigNodeSnapshot] Load snapshot success, latestSnapshotRootDir: {}",
          latestSnapshotRootDir);
    }
  }

  private DataSet getSchemaNodeManagementPartition(ConfigPhysicalPlan req) {
    int level;
    PartialPath partialPath;
    Set<TSchemaNode> alreadyMatchedNode;
    Set<PartialPath> needMatchedNode;
    List<String> matchedStorageGroups = new ArrayList<>();

    GetNodePathsPartitionPlan getNodePathsPartitionPlan = (GetNodePathsPartitionPlan) req;
    partialPath = getNodePathsPartitionPlan.getPartialPath();
    level = getNodePathsPartitionPlan.getLevel();
    if (-1 == level) {
      // get child paths
      Pair<Set<TSchemaNode>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getChildNodePathInNextLevel(
              partialPath, getNodePathsPartitionPlan.getScope());
      alreadyMatchedNode = matchedChildInNextLevel.left;
      if (!partialPath.hasMultiLevelMatchWildcard()) {
        needMatchedNode = new HashSet<>();
        for (PartialPath databasePath : matchedChildInNextLevel.right) {
          if (databasePath.getNodeLength() == partialPath.getNodeLength() + 1) {
            // this database node is already the target child node, no need to traverse its
            // schemaengine
            // region
            continue;
          }
          needMatchedNode.add(databasePath);
        }
      } else {
        needMatchedNode = matchedChildInNextLevel.right;
      }
    } else {
      // count nodes
      Pair<List<PartialPath>, Set<PartialPath>> matchedChildInNextLevel =
          clusterSchemaInfo.getNodesListInGivenLevel(
              partialPath, level, getNodePathsPartitionPlan.getScope());
      alreadyMatchedNode =
          matchedChildInNextLevel.left.stream()
              .map(path -> new TSchemaNode(path.getFullPath(), MNodeType.UNIMPLEMENT.getNodeType()))
              .collect(Collectors.toSet());
      needMatchedNode = matchedChildInNextLevel.right;
    }

    needMatchedNode.forEach(nodePath -> matchedStorageGroups.add(nodePath.getFullPath()));
    SchemaNodeManagementResp schemaNodeManagementResp =
        (SchemaNodeManagementResp)
            partitionInfo.getSchemaNodeManagementPartition(matchedStorageGroups);
    if (schemaNodeManagementResp.getStatus().getCode()
        == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      schemaNodeManagementResp.setMatchedNode(alreadyMatchedNode);
    }
    return schemaNodeManagementResp;
  }

  private DataSet getRegionInfoList(ConfigPhysicalPlan req) {
    final GetRegionInfoListPlan getRegionInfoListPlan = (GetRegionInfoListPlan) req;
    TShowRegionReq showRegionReq = getRegionInfoListPlan.getShowRegionReq();
    if (showRegionReq != null && showRegionReq.isSetDatabases()) {
      final List<String> storageGroups = showRegionReq.getDatabases();
      final List<String> matchedStorageGroups =
          clusterSchemaInfo.getMatchedDatabaseSchemasByName(storageGroups).values().stream()
              .map(TDatabaseSchema::getName)
              .collect(Collectors.toList());
      if (!matchedStorageGroups.isEmpty()) {
        showRegionReq.setDatabases(matchedStorageGroups);
      }
    }
    return partitionInfo.getRegionInfoList(getRegionInfoListPlan);
  }
}
