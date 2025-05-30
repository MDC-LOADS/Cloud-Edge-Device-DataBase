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

package org.apache.iotdb.db.service;

import org.apache.iotdb.common.rpc.thrift.TConfigNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TConsensusGroupType;
import org.apache.iotdb.common.rpc.thrift.TDataNodeConfiguration;
import org.apache.iotdb.common.rpc.thrift.TDataNodeLocation;
import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TNodeResource;
import org.apache.iotdb.commons.client.exception.ClientManagerException;
import org.apache.iotdb.commons.concurrent.IoTDBDefaultThreadExceptionHandler;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.pipe.plugin.meta.PipePluginMeta;
import org.apache.iotdb.commons.service.JMXService;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.service.metric.MetricService;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.commons.trigger.service.TriggerExecutableManager;
import org.apache.iotdb.commons.udf.UDFInformation;
import org.apache.iotdb.commons.udf.service.UDFClassLoaderManager;
import org.apache.iotdb.commons.udf.service.UDFExecutableManager;
import org.apache.iotdb.commons.udf.service.UDFManagementService;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRegisterResp;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartReq;
import org.apache.iotdb.confignode.rpc.thrift.TDataNodeRestartResp;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListReq;
import org.apache.iotdb.confignode.rpc.thrift.TGetJarInListResp;
import org.apache.iotdb.confignode.rpc.thrift.TNodeVersionInfo;
import org.apache.iotdb.confignode.rpc.thrift.TRuntimeConfiguration;
import org.apache.iotdb.confignode.rpc.thrift.TSystemConfigurationResp;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.DataNodeStartupCheck;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.conf.IoTDBStartCheck;
import org.apache.iotdb.db.conf.rest.IoTDBRestServiceDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.consensus.SchemaRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeAgent;
import org.apache.iotdb.db.protocol.client.ConfigNodeClient;
import org.apache.iotdb.db.protocol.client.ConfigNodeClientManager;
import org.apache.iotdb.db.protocol.client.ConfigNodeInfo;
import org.apache.iotdb.db.protocol.thrift.impl.ClientRPCServiceImpl;
import org.apache.iotdb.db.protocol.thrift.impl.DataNodeRegionManager;
import org.apache.iotdb.db.queryengine.execution.exchange.MPPDataExchangeService;
import org.apache.iotdb.db.queryengine.execution.schedule.DriverScheduler;
import org.apache.iotdb.db.queryengine.plan.execution.ServerStart;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.service.metrics.DataNodeMetricsHelper;
import org.apache.iotdb.db.service.metrics.IoTDBInternalLocalReporter;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.db.storageengine.buffer.CacheHitRatioMonitor;
import org.apache.iotdb.db.storageengine.dataregion.compaction.schedule.CompactionTaskManager;
import org.apache.iotdb.db.storageengine.dataregion.flush.FlushManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.WALManager;
import org.apache.iotdb.db.storageengine.dataregion.wal.utils.WALMode;
import org.apache.iotdb.db.storageengine.rescon.disk.TierManager;
import org.apache.iotdb.db.trigger.executor.TriggerExecutor;
import org.apache.iotdb.db.trigger.service.TriggerInformationUpdater;
import org.apache.iotdb.db.trigger.service.TriggerManagementService;
import org.apache.iotdb.metrics.config.MetricConfigDescriptor;
import org.apache.iotdb.metrics.utils.InternalReporterType;
import org.apache.iotdb.rpc.TSStatusCode;
import org.apache.iotdb.udf.api.exception.UDFManagementException;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.DEFAULT_CLUSTER_NAME;

public class DataNode implements DataNodeMBean {

  private static final Logger logger = LoggerFactory.getLogger(DataNode.class);
  private static final IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private final String mbeanName =
          String.format(
                  "%s:%s=%s",
                  IoTDBConstant.IOTDB_SERVICE_JMX_NAME,
                  IoTDBConstant.JMX_TYPE,
                  ServiceType.DATA_NODE.getJmxName());

  private static final File SYSTEM_PROPERTIES =
          SystemFileFactory.INSTANCE.getFile(
                  config.getSystemDir() + File.separator + IoTDBStartCheck.PROPERTIES_FILE_NAME);

  /**
   * When joining a cluster or getting configuration this node will retry at most "DEFAULT_RETRY"
   * times before returning a failure to the client.
   */
  private static final int DEFAULT_RETRY = 10;

  private static final long DEFAULT_RETRY_INTERVAL_IN_MS = config.getJoinClusterRetryIntervalMs();

  private final TEndPoint thisNode = new TEndPoint();

  /** Hold the information of trigger, udf...... */
  private final ResourcesInformationHolder resourcesInformationHolder =
          new ResourcesInformationHolder();

  /** Responsible for keeping trigger information up to date. */
  private final TriggerInformationUpdater triggerInformationUpdater =
          new TriggerInformationUpdater();

  private static final String REGISTER_INTERRUPTION =
          "Unexpected interruption when waiting to register to the cluster";

  private boolean schemaRegionConsensusStarted = false;
  private boolean dataRegionConsensusStarted = false;

  private DataNode() {
    // We do not init anything here, so that we can re-initialize the instance in IT.
  }

  private static final RegisterManager registerManager = new RegisterManager();

  public static DataNode getInstance() {
    return DataNodeHolder.INSTANCE;
  }

  public static void main(String[] args) {
    logger.info("IoTDB-DataNode environment variables: {}", IoTDBConfig.getEnvironmentVariables());
    logger.info("IoTDB-DataNode default charset is: {}", Charset.defaultCharset().displayName());
    //新建thrift服务器
//    System.out.println("ok");
    Thread serverThread = new Thread(new ServerRunnable());//启动服务器
    serverThread.start();
    Thread IOMonitorThread = new Thread(new MonitorRunnable());//启动服务器
    IOMonitorThread.start();

    new DataNodeServerCommandLine().doMain(args);
  }

  protected void doAddNode() {
    boolean isFirstStart = false;
    try {
      // Check if this DataNode is start for the first time and do other pre-checks
      isFirstStart = prepareDataNode();

      if (isFirstStart) {
        // Set target ConfigNodeList from iotdb-datanode.properties file
        ConfigNodeInfo.getInstance()
                .updateConfigNodeList(Collections.singletonList(config.getSeedConfigNode()));
      } else {
        // Load registered ConfigNodes from system.properties
        ConfigNodeInfo.getInstance().loadConfigNodeList();
      }

      // Pull and check system configurations from ConfigNode-leader
      pullAndCheckSystemConfigurations();

      if (isFirstStart) {
        // Register this DataNode to the cluster when first start
        sendRegisterRequestToConfigNode();
      } else {
        // Send restart request of this DataNode
        sendRestartRequestToConfigNode();
      }
      // TierManager need DataNodeId to do some operations so the reset method need to be invoked
      // after DataNode adding
      TierManager.getInstance().resetFolders();
      // Active DataNode
      active();

      // Setup metric service
      setUpMetricService();

      // Setup rpc service
      setUpRPCService();

      // Serialize mutable system properties
      IoTDBStartCheck.getInstance().serializeMutableSystemPropertiesIfNecessary();

      logger.info("IoTDB configuration: {}", config.getConfigMessage());
      logger.info("Congratulation, IoTDB DataNode is set up successfully. Now, enjoy yourself!");

    } catch (StartupException | IOException e) {
      logger.error("Fail to start server", e);
      if (isFirstStart) {
        // Delete the system.properties file when first start failed.
        // Therefore, the next time this DataNode is start will still be seen as the first time.
        SYSTEM_PROPERTIES.deleteOnExit();
      }
      stop();
      System.exit(-1);
    }
  }

  /** Prepare cluster IoTDB-DataNode */
  private boolean prepareDataNode() throws StartupException, IOException {
    long startTime = System.currentTimeMillis();
    // Set cluster mode
    config.setClusterMode(true);

    // Notice: Consider this DataNode as first start if the system.properties file doesn't exist
    IoTDBStartCheck.getInstance().checkOldSystemConfig();
    boolean isFirstStart = IoTDBStartCheck.getInstance().checkIsFirstStart();

    // Set this node
    thisNode.setIp(config.getInternalAddress());
    thisNode.setPort(config.getInternalPort());

    // Startup checks
    DataNodeStartupCheck checks = new DataNodeStartupCheck(IoTDBConstant.DN_ROLE, config);
    checks.startUpCheck();
    long endTime = System.currentTimeMillis();
    logger.info("The DataNode is prepared successfully, which takes {} ms", (endTime - startTime));
    return isFirstStart;
  }

  /**
   * Pull and check the following system configurations:
   *
   * <p>1. GlobalConfig
   *
   * <p>2. RatisConfig
   *
   * <p>3. CQConfig
   *
   * @throws StartupException When failed connect to ConfigNode-leader
   */
  private void pullAndCheckSystemConfigurations() throws StartupException {
    logger.info("Pulling system configurations from the ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    /* Pull system configurations */
    int retry = DEFAULT_RETRY;
    TSystemConfigurationResp configurationResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
                   ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        configurationResp = configNodeClient.getSystemConfiguration();
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn(
                "Cannot pull system configurations from ConfigNode-leader, because: {}",
                e.getMessage());
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (configurationResp == null
            || !configurationResp.isSetStatus()
            || configurationResp.getStatus().getCode() != TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      // All tries failed
      logger.error(
              "Cannot pull system configurations from ConfigNode-leader after {} retries",
              DEFAULT_RETRY);
      throw new StartupException("Cannot pull system configurations from ConfigNode-leader");
    }

    /* Load system configurations */
    IoTDBDescriptor.getInstance().loadGlobalConfig(configurationResp.globalConfig);
    IoTDBDescriptor.getInstance().loadRatisConfig(configurationResp.ratisConfig);
    IoTDBDescriptor.getInstance().loadCQConfig(configurationResp.cqConfig);
    CommonDescriptor.getInstance().loadGlobalConfig(configurationResp.globalConfig);

    /* Set cluster consensus protocol class */
    if (!IoTDBStartCheck.getInstance()
            .checkConsensusProtocolExists(TConsensusGroupType.DataRegion)) {
      config.setDataRegionConsensusProtocolClass(
              configurationResp.globalConfig.getDataRegionConsensusProtocolClass());
    }

    if (!IoTDBStartCheck.getInstance()
            .checkConsensusProtocolExists(TConsensusGroupType.SchemaRegion)) {
      config.setSchemaRegionConsensusProtocolClass(
              configurationResp.globalConfig.getSchemaRegionConsensusProtocolClass());
    }

    /* Check system configurations */
    try {
      IoTDBStartCheck.getInstance().checkSystemConfig();
      IoTDBStartCheck.getInstance().checkDirectory();
      if (!config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.IOT_CONSENSUS)) {
        // In current implementation, only IoTConsensus need separated memory from Consensus
        IoTDBDescriptor.getInstance().reclaimConsensusMemory();
      }
    } catch (Exception e) {
      throw new StartupException(e.getMessage());
    }
    long endTime = System.currentTimeMillis();
    logger.info(
            "Successfully pull system configurations from ConfigNode-leader, which takes {} ms",
            (endTime - startTime));
  }

  /**
   * Store runtime configurations, which includes:
   *
   * <p>1. All ConfigNodes in cluster
   *
   * <p>2. All template information
   *
   * <p>3. All UDF information
   *
   * <p>4. All trigger information
   *
   * <p>5. All Pipe information
   *
   * <p>6. All TTL information
   */
  private void storeRuntimeConfigurations(
          List<TConfigNodeLocation> configNodeLocations, TRuntimeConfiguration runtimeConfiguration) {
    /* Store ConfigNodeList */
    List<TEndPoint> configNodeList = new ArrayList<>();
    for (TConfigNodeLocation configNodeLocation : configNodeLocations) {
      configNodeList.add(configNodeLocation.getInternalEndPoint());
    }
    ConfigNodeInfo.getInstance().updateConfigNodeList(configNodeList);

    /* Store templateSetInfo */
    ClusterTemplateManager.getInstance()
            .updateTemplateSetInfo(runtimeConfiguration.getTemplateInfo());

    /* Store udfInformationList */
    getUDFInformationList(runtimeConfiguration.getAllUDFInformation());

    /* Store triggerInformationList */
    getTriggerInformationList(runtimeConfiguration.getAllTriggerInformation());

    /* Store pipeInformationList */
    getPipeInformationList(runtimeConfiguration.getAllPipeInformation());

    /* Store ttl information */
    StorageEngine.getInstance().updateTTLInfo(runtimeConfiguration.getAllTTLInformation());
  }

  /**
   * Register this DataNode into cluster.
   *
   * @throws StartupException if register failed.
   * @throws IOException if serialize cluster name and dataNode Id failed.
   */
  private void sendRegisterRequestToConfigNode() throws StartupException, IOException {
    logger.info("Sending register request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    /* Send register request */
    int retry = DEFAULT_RETRY;
    TDataNodeRegisterReq req = new TDataNodeRegisterReq();
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    req.setClusterName(config.getClusterName());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    TDataNodeRegisterResp dataNodeRegisterResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
                   ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRegisterResp = configNodeClient.registerDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn("Cannot register to the cluster, because: {}", e.getMessage());
        retry--;
      }

      try {
        // Wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (dataNodeRegisterResp == null) {
      // All tries failed
      logger.error(
              "Cannot register into cluster after {} retries. "
                      + "Please check dn_seed_config_node in iotdb-datanode.properties.",
              DEFAULT_RETRY);
      throw new StartupException("Cannot register into the cluster.");
    }

    if (dataNodeRegisterResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {

      /* Store runtime configurations when register success */
      int dataNodeID = dataNodeRegisterResp.getDataNodeId();
      config.setDataNodeId(dataNodeID);
      IoTDBStartCheck.getInstance()
              .serializeClusterNameAndDataNodeId(config.getClusterName(), dataNodeID);

      storeRuntimeConfigurations(
              dataNodeRegisterResp.getConfigNodeList(), dataNodeRegisterResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();
      logger.info(
              "Successfully register to the cluster: {} , which takes {} ms.",
              config.getClusterName(),
              (endTime - startTime));
    } else {
      /* Throw exception when register failed */
      logger.error(dataNodeRegisterResp.getStatus().getMessage());
      throw new StartupException("Cannot register to the cluster.");
    }
  }

  private void sendRestartRequestToConfigNode() throws StartupException {
    logger.info("Sending restart request to ConfigNode-leader...");
    long startTime = System.currentTimeMillis();
    /* Send restart request */
    int retry = DEFAULT_RETRY;
    TDataNodeRestartReq req = new TDataNodeRestartReq();
    req.setClusterName(
            config.getClusterName() == null ? DEFAULT_CLUSTER_NAME : config.getClusterName());
    req.setDataNodeConfiguration(generateDataNodeConfiguration());
    req.setVersionInfo(new TNodeVersionInfo(IoTDBConstant.VERSION, IoTDBConstant.BUILD_INFO));
    TDataNodeRestartResp dataNodeRestartResp = null;
    while (retry > 0) {
      try (ConfigNodeClient configNodeClient =
                   ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
        dataNodeRestartResp = configNodeClient.restartDataNode(req);
        break;
      } catch (TException | ClientManagerException e) {
        logger.warn(
                "Cannot send restart request to the ConfigNode-leader, because: {}", e.getMessage());
        retry--;
      }

      try {
        // wait to start the next try
        Thread.sleep(DEFAULT_RETRY_INTERVAL_IN_MS);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        logger.warn(REGISTER_INTERRUPTION, e);
        retry = -1;
      }
    }
    if (dataNodeRestartResp == null) {
      // All tries failed
      logger.error(
              "Cannot send restart DataNode request to ConfigNode-leader after {} retries. "
                      + "Please check dn_seed_config_node in iotdb-datanode.properties.",
              DEFAULT_RETRY);
      throw new StartupException("Cannot send restart DataNode request to ConfigNode-leader.");
    }

    if (dataNodeRestartResp.getStatus().getCode() == TSStatusCode.SUCCESS_STATUS.getStatusCode()) {
      /* Store runtime configurations when restart request is accepted */
      storeRuntimeConfigurations(
              dataNodeRestartResp.getConfigNodeList(), dataNodeRestartResp.getRuntimeConfiguration());
      long endTime = System.currentTimeMillis();
      logger.info(
              "Restart request to cluster: {} is accepted, which takes {} ms.",
              config.getClusterName(),
              (endTime - startTime));
    } else {
      /* Throw exception when restart is rejected */
      throw new StartupException(dataNodeRestartResp.getStatus().getMessage());
    }
  }

  private void prepareResources() throws StartupException {
    prepareUDFResources();
    prepareTriggerResources();
    preparePipeResources();
  }

  /**
   * Register services and set up DataNode.
   *
   * @throws StartupException if start up failed.
   */
  private void active() throws StartupException {
    try {
      processPid();
      setUp();
    } catch (StartupException e) {
      logger.error("Meet error while starting up.", e);
      throw new StartupException("Error in activating IoTDB DataNode.");
    }
    logger.info("IoTDB DataNode has started.");

    try {
      long startTime = System.currentTimeMillis();
      SchemaRegionConsensusImpl.getInstance().start();
      long schemaRegionEndTime = System.currentTimeMillis();
      logger.info(
              "SchemaRegion consensus start successfully, which takes {} ms.",
              (schemaRegionEndTime - startTime));
      schemaRegionConsensusStarted = true;
      DataRegionConsensusImpl.getInstance().start();
      long dataRegionEndTime = System.currentTimeMillis();
      logger.info(
              "DataRegion consensus start successfully, which takes {} ms.",
              (dataRegionEndTime - schemaRegionEndTime));
      dataRegionConsensusStarted = true;
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  void processPid() {
    String pidFile = System.getProperty(IoTDBConstant.IOTDB_PIDFILE);
    if (pidFile != null) {
      new File(pidFile).deleteOnExit();
    }
  }

  private void setUp() throws StartupException {
    logger.info("Setting up IoTDB DataNode...");
    registerManager.register(new JMXService());
    JMXService.registerMBean(getInstance(), mbeanName);

    // Get resources for trigger,udf,pipe...
    prepareResources();

    Runtime.getRuntime().addShutdownHook(new IoTDBShutdownHook());
    setUncaughtExceptionHandler();

    logger.info("Recover the schema...");
    initSchemaEngine();
    registerManager.register(FlushManager.getInstance());
    registerManager.register(CacheHitRatioMonitor.getInstance());

    // Close wal when using ratis consensus
    if (config.isClusterMode()
            && config.getDataRegionConsensusProtocolClass().equals(ConsensusFactory.RATIS_CONSENSUS)) {
      config.setWalMode(WALMode.DISABLE);
    }
    registerManager.register(WALManager.getInstance());

    // In mpp mode we need to start some other services
    registerManager.register(StorageEngine.getInstance());
    registerManager.register(MPPDataExchangeService.getInstance());
    registerManager.register(DriverScheduler.getInstance());

    registerUdfServices();

    logger.info(
            "IoTDB DataNode is setting up, some databases may not be ready now, please wait several seconds...");
    long startTime = System.currentTimeMillis();
    while (!StorageEngine.getInstance().isAllSgReady()) {
      try {
        TimeUnit.MILLISECONDS.sleep(1000);
      } catch (InterruptedException e) {
        logger.warn("IoTDB DataNode failed to set up.", e);
        Thread.currentThread().interrupt();
        return;
      }
    }
    long endTime = System.currentTimeMillis();
    logger.info("Wait for all databases ready, which takes {} ms.", (endTime - startTime));
    // Must init after SchemaEngine and StorageEngine prepared well
    DataNodeRegionManager.getInstance().init();

    // Start region migrate service
    registerManager.register(RegionMigrateService.getInstance());

    registerManager.register(CompactionTaskManager.getInstance());

    registerManager.register(PipeAgent.runtime());
  }

  /** Set up RPC and protocols after DataNode is available */
  private void setUpRPCService() throws StartupException {
    // Start InternalRPCService to indicate that the current DataNode can accept cluster scheduling
    registerManager.register(DataNodeInternalRPCService.getInstance());

    // Notice: During the period between starting the internal RPC service
    // and starting the client RPC service , some requests may fail because
    // DataNode is not marked as RUNNING by ConfigNode-leader yet.

    // Start client RPCService to indicate that the current DataNode provide external services
    IoTDBDescriptor.getInstance()
            .getConfig()
            .setRpcImplClassName(ClientRPCServiceImpl.class.getName());
    if (config.isEnableRpcService()) {
      registerManager.register(RPCService.getInstance());
    }
    // init service protocols
    initProtocols();
  }

  private void setUpMetricService() throws StartupException {
    MetricConfigDescriptor.getInstance().getMetricConfig().setNodeId(config.getDataNodeId());
    registerManager.register(MetricService.getInstance());

    // init metric service
    if (MetricConfigDescriptor.getInstance()
            .getMetricConfig()
            .getInternalReportType()
            .equals(InternalReporterType.IOTDB)) {
      MetricService.getInstance().updateInternalReporter(new IoTDBInternalLocalReporter());
    }
    MetricService.getInstance().startInternalReporter();
    // bind predefined metrics
    DataNodeMetricsHelper.bind();
  }

  public static TDataNodeLocation generateDataNodeLocation() {
    TDataNodeLocation location = new TDataNodeLocation();
    location.setDataNodeId(config.getDataNodeId());
    location.setClientRpcEndPoint(new TEndPoint(config.getRpcAddress(), config.getRpcPort()));
    location.setInternalEndPoint(
            new TEndPoint(config.getInternalAddress(), config.getInternalPort()));
    location.setMPPDataExchangeEndPoint(
            new TEndPoint(config.getInternalAddress(), config.getMppDataExchangePort()));
    location.setDataRegionConsensusEndPoint(
            new TEndPoint(config.getInternalAddress(), config.getDataRegionConsensusPort()));
    location.setSchemaRegionConsensusEndPoint(
            new TEndPoint(config.getInternalAddress(), config.getSchemaRegionConsensusPort()));
    return location;
  }

  /**
   * Generate dataNodeConfiguration. Warning: Don't private this method !!!
   *
   * @return TDataNodeConfiguration
   */
  public TDataNodeConfiguration generateDataNodeConfiguration() {
    // Set DataNodeLocation
    TDataNodeLocation location = generateDataNodeLocation();

    // Set NodeResource
    TNodeResource resource = new TNodeResource();
    resource.setCpuCoreNum(Runtime.getRuntime().availableProcessors());
    resource.setMaxMemory(Runtime.getRuntime().totalMemory());

    return new TDataNodeConfiguration(location, resource);
  }

  private void registerUdfServices() throws StartupException {
    registerManager.register(TemporaryQueryDataFileService.getInstance());
    registerManager.register(UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir()));
  }

  private void initUDFRelatedInstance() throws StartupException {
    try {
      UDFExecutableManager.setupAndGetInstance(config.getUdfTemporaryLibDir(), config.getUdfDir());
      UDFClassLoaderManager.setupAndGetInstance(config.getUdfDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareUDFResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    initUDFRelatedInstance();
    if (resourcesInformationHolder.getUDFInformationList() == null
            || resourcesInformationHolder.getUDFInformationList().isEmpty()) {
      return;
    }

    // Get jars from config node
    List<UDFInformation> udfNeedJarList = getJarListForUDF();
    int index = 0;
    while (index < udfNeedJarList.size()) {
      List<UDFInformation> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
              && index + offset < udfNeedJarList.size()) {
        curList.add(udfNeedJarList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      getJarOfUDFs(curList);
    }

    // Create instances of udf and do registration
    try {
      for (UDFInformation udfInformation : resourcesInformationHolder.getUDFInformationList()) {
        UDFManagementService.getInstance().doRegister(udfInformation);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }
    long endTime = System.currentTimeMillis();
    logger.debug("successfully registered all the UDFs, which takes {} ms.", (endTime - startTime));
    if (logger.isDebugEnabled()) {
      for (UDFInformation udfInformation :
              UDFManagementService.getInstance().getAllUDFInformation()) {
        logger.debug("get udf: {}", udfInformation.getFunctionName());
      }
    }
  }

  private void getJarOfUDFs(List<UDFInformation> udfInformationList) throws StartupException {
    try (ConfigNodeClient configNodeClient =
                 ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
              udfInformationList.stream().map(UDFInformation::getJarName).collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getUDFJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get UDF jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < udfInformationList.size(); i++) {
        UDFExecutableManager.getInstance()
                .saveToInstallDir(jarList.get(i), udfInformationList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  /** Generate a list for UDFs that do not have jar on this node. */
  private List<UDFInformation> getJarListForUDF() {
    List<UDFInformation> res = new ArrayList<>();
    for (UDFInformation udfInformation : resourcesInformationHolder.getUDFInformationList()) {
      if (udfInformation.isUsingURI()) {
        // Jar does not exist, add current udfInformation to list
        if (!UDFExecutableManager.getInstance()
                .hasFileUnderInstallDir(udfInformation.getJarName())) {
          res.add(udfInformation);
        } else {
          try {
            // Local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (UDFManagementService.getInstance().isLocalJarConflicted(udfInformation)) {
              res.add(udfInformation);
            }
          } catch (UDFManagementException e) {
            res.add(udfInformation);
          }
        }
      }
    }
    return res;
  }

  private void getUDFInformationList(List<ByteBuffer> allUDFInformation) {
    if (allUDFInformation != null && !allUDFInformation.isEmpty()) {
      List<UDFInformation> list = new ArrayList<>();
      for (ByteBuffer UDFInformationByteBuffer : allUDFInformation) {
        list.add(UDFInformation.deserialize(UDFInformationByteBuffer));
      }
      resourcesInformationHolder.setUDFInformationList(list);
    }
  }

  private void initTriggerRelatedInstance() throws StartupException {
    try {
      TriggerExecutableManager.setupAndGetInstance(
              config.getTriggerTemporaryLibDir(), config.getTriggerDir());
    } catch (IOException e) {
      throw new StartupException(e);
    }
  }

  private void prepareTriggerResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    initTriggerRelatedInstance();
    if (resourcesInformationHolder.getTriggerInformationList() == null
            || resourcesInformationHolder.getTriggerInformationList().isEmpty()) {
      return;
    }

    // Get jars from config node
    List<TriggerInformation> triggerNeedJarList = getJarListForTrigger();
    int index = 0;
    while (index < triggerNeedJarList.size()) {
      List<TriggerInformation> curList = new ArrayList<>();
      int offset = 0;
      while (offset < ResourcesInformationHolder.getJarNumOfOneRpc()
              && index + offset < triggerNeedJarList.size()) {
        curList.add(triggerNeedJarList.get(index + offset));
        offset++;
      }
      index += (offset + 1);
      getJarOfTriggers(curList);
    }

    // Create instances of triggers and do registration
    try {
      for (TriggerInformation triggerInformation :
              resourcesInformationHolder.getTriggerInformationList()) {
        TriggerManagementService.getInstance().doRegister(triggerInformation, true);
      }
    } catch (Exception e) {
      throw new StartupException(e);
    }

    if (logger.isDebugEnabled()) {
      for (TriggerInformation triggerInformation :
              TriggerManagementService.getInstance().getAllTriggerInformationInTriggerTable()) {
        logger.debug("get trigger: {}", triggerInformation.getTriggerName());
      }
      for (TriggerExecutor triggerExecutor :
              TriggerManagementService.getInstance().getAllTriggerExecutors()) {
        logger.debug(
                "get trigger executor: {}", triggerExecutor.getTriggerInformation().getTriggerName());
      }
    }
    // Start TriggerInformationUpdater
    triggerInformationUpdater.startTriggerInformationUpdater();
    long endTime = System.currentTimeMillis();
    logger.info(
            "successfully registered all the triggers, which takes {} ms.", (endTime - startTime));
  }

  private void getJarOfTriggers(List<TriggerInformation> triggerInformationList)
          throws StartupException {
    try (ConfigNodeClient configNodeClient =
                 ConfigNodeClientManager.getInstance().borrowClient(ConfigNodeInfo.CONFIG_REGION_ID)) {
      List<String> jarNameList =
              triggerInformationList.stream()
                      .map(TriggerInformation::getJarName)
                      .collect(Collectors.toList());
      TGetJarInListResp resp = configNodeClient.getTriggerJar(new TGetJarInListReq(jarNameList));
      if (resp.getStatus().getCode() == TSStatusCode.EXECUTE_STATEMENT_ERROR.getStatusCode()) {
        throw new StartupException("Failed to get trigger jar from config node.");
      }
      List<ByteBuffer> jarList = resp.getJarList();
      for (int i = 0; i < triggerInformationList.size(); i++) {
        TriggerExecutableManager.getInstance()
                .saveToInstallDir(jarList.get(i), triggerInformationList.get(i).getJarName());
      }
    } catch (IOException | TException | ClientManagerException e) {
      throw new StartupException(e);
    }
  }

  /** Generate a list for triggers that do not have jar on this node. */
  private List<TriggerInformation> getJarListForTrigger() {
    List<TriggerInformation> res = new ArrayList<>();
    for (TriggerInformation triggerInformation :
            resourcesInformationHolder.getTriggerInformationList()) {
      if (triggerInformation.isUsingURI()) {
        // jar does not exist, add current triggerInformation to list
        if (!TriggerExecutableManager.getInstance()
                .hasFileUnderInstallDir(triggerInformation.getJarName())) {
          res.add(triggerInformation);
        } else {
          try {
            // local jar has conflicts with jar on config node, add current triggerInformation to
            // list
            if (TriggerManagementService.getInstance().isLocalJarConflicted(triggerInformation)) {
              res.add(triggerInformation);
            }
          } catch (TriggerManagementException e) {
            res.add(triggerInformation);
          }
        }
      }
    }
    return res;
  }

  private void getTriggerInformationList(List<ByteBuffer> allTriggerInformation) {
    if (allTriggerInformation != null && !allTriggerInformation.isEmpty()) {
      List<TriggerInformation> list = new ArrayList<>();
      for (ByteBuffer triggerInformationByteBuffer : allTriggerInformation) {
        list.add(TriggerInformation.deserialize(triggerInformationByteBuffer));
      }
      resourcesInformationHolder.setTriggerInformationList(list);
    }
  }

  private void preparePipeResources() throws StartupException {
    long startTime = System.currentTimeMillis();
    PipeAgent.runtime().preparePipeResources(resourcesInformationHolder);
    long endTime = System.currentTimeMillis();
    logger.info("Prepare pipe resources successfully, which takes {} ms.", (endTime - startTime));
  }

  private void getPipeInformationList(List<ByteBuffer> allPipeInformation) {
    final List<PipePluginMeta> list = new ArrayList<>();
    if (allPipeInformation != null) {
      for (ByteBuffer pipeInformationByteBuffer : allPipeInformation) {
        list.add(PipePluginMeta.deserialize(pipeInformationByteBuffer));
      }
    }
    resourcesInformationHolder.setPipePluginMetaList(list);
  }

  private void initSchemaEngine() {
    long startTime = System.currentTimeMillis();
    SchemaEngine.getInstance().init();
    long endTime = System.currentTimeMillis();
    logger.info("Recover schema successfully, which takes {} ms.", (endTime - startTime));
  }

  public void stop() {
    deactivate();
    SchemaEngine.getInstance().clear();
    try {
      MetricService.getInstance().stop();
      if (schemaRegionConsensusStarted) {
        SchemaRegionConsensusImpl.getInstance().stop();
      }
      if (dataRegionConsensusStarted) {
        DataRegionConsensusImpl.getInstance().stop();
      }
    } catch (Exception e) {
      logger.error("Stop data node error", e);
    }
  }

  private void initProtocols() throws StartupException {
    if (config.isEnableMQTTService()) {
      registerManager.register(MQTTService.getInstance());
    }
    if (IoTDBRestServiceDescriptor.getInstance().getConfig().isEnableRestService()) {
      registerManager.register(RestService.getInstance());
    }
    if (PipeConfig.getInstance().getPipeAirGapReceiverEnabled()) {
      registerManager.register(PipeAgent.receiver().airGap());
    }
  }

  private void deactivate() {
    logger.info("Deactivating IoTDB DataNode...");
    stopTriggerRelatedServices();
    registerManager.deregisterAll();
    JMXService.deregisterMBean(mbeanName);
    logger.info("IoTDB DataNode is deactivated.");
  }

  private void stopTriggerRelatedServices() {
    triggerInformationUpdater.stopTriggerInformationUpdater();
  }

  private void setUncaughtExceptionHandler() {
    Thread.setDefaultUncaughtExceptionHandler(new IoTDBDefaultThreadExceptionHandler());
  }

  private static class DataNodeHolder {

    private static final DataNode INSTANCE = new DataNode();

    private DataNodeHolder() {
      // Empty constructor
    }
  }
}
class ServerRunnable implements Runnable {
  @Override
  public void run() {
    // 创建并启动服务器
    ServerStart server = new ServerStart();
    server.start();
//    PipeInfo pipeInfo=new PipeInfo();
  }
}
class MonitorRunnable implements Runnable {
  @Override
  public void run() {
//    LoadDetection pipe = new LoadDetection();
//    for(int i=20;i>0;i--)
//    {
//      try {
//        Thread.sleep(1000);//时间
//        System.out.println("waiting start"+i);
//      } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//      }
//    }
//    pipe.PipeStart();
//    for(int i=2;i>0;i--)
//    {
//      try {
//        Thread.sleep(1000);//时间
//        System.out.println("waiting stop"+i);
//      } catch (InterruptedException e) {
//        throw new RuntimeException(e);
//      }
//    }

//    try {
//      Thread.sleep(3000);//时间
//      System.out.println("waiting stop");
//    } catch (InterruptedException e) {
//      throw new RuntimeException(e);
//    }

//    pipe.PipeStop();
    // 启动io监测线程


    IOMonitor monitor = new IOMonitor(125);
    try {
        monitor.monitor();
//        Thread.sleep(5000);
    } catch (InterruptedException | IOException e) {
      throw new RuntimeException(e);
    }


  }
}
