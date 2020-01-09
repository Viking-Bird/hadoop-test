/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.yarn.server.resourcemanager.recovery;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputByteBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.delegation.DelegationKey;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerCommonProtos.VersionProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.AMRMTokenSecretManagerStateProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationAttemptStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.ApplicationStateDataProto;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerRecoveryProtos.EpochProto;
import org.apache.hadoop.yarn.security.client.RMDelegationTokenIdentifier;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.hadoop.yarn.server.records.impl.pb.VersionPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.RMZKUtils;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.*;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.AMRMTokenSecretManagerStatePBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationAttemptStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.ApplicationStateDataPBImpl;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.records.impl.pb.EpochPBImpl;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.io.*;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Changes from 1.1 to 1.2, AMRMTokenSecretManager state has been saved
 * separately. The currentMasterkey and nextMasterkey have been stored.
 * Also, AMRMToken has been removed from ApplicationAttemptState.
 */
@Private
@Unstable
public class ZKRMStateStore extends RMStateStore {

    public static final Log LOG = LogFactory.getLog(ZKRMStateStore.class);
    private final SecureRandom random = new SecureRandom();

    protected static final String ROOT_ZNODE_NAME = "ZKRMStateRoot";
    protected static final Version CURRENT_VERSION_INFO = Version
            .newInstance(1, 2);
    private static final String RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME =
            "RMDelegationTokensRoot";
    private static final String RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME =
            "RMDTSequentialNumber";
    private static final String RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME =
            "RMDTMasterKeysRoot";
    private int numRetries;

    private String zkHostPort = null;
    private int zkSessionTimeout;

    @VisibleForTesting
    long zkRetryInterval;
    private List<ACL> zkAcl;
    private List<ZKUtil.ZKAuthInfo> zkAuths;

    class ZKSyncOperationCallback implements AsyncCallback.VoidCallback {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            if (rc == Code.OK.intValue()) {
                LOG.info("ZooKeeper sync operation succeeded. path: " + path);
            } else {
                LOG.fatal("ZooKeeper sync operation failed. Waiting for session " +
                        "timeout. path: " + path);
            }
        }
    }

    /**
     * ROOT_DIR_PATH
     * |--- VERSION_INFO
     * |--- EPOCH_NODE
     * |--- RM_ZK_FENCING_LOCK
     * |--- RM_APP_ROOT
     * |     |----- (#ApplicationId1)
     * |     |        |----- (#ApplicationAttemptIds)
     * |     |
     * |     |----- (#ApplicationId2)
     * |     |       |----- (#ApplicationAttemptIds)
     * |     ....
     * |
     * |--- RM_DT_SECRET_MANAGER_ROOT
     * |----- RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME
     * |----- RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME
     * |       |----- Token_1
     * |       |----- Token_2
     * |       ....
     * |
     * |----- RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME
     * |      |----- Key_1
     * |      |----- Key_2
     * ....
     * |--- AMRMTOKEN_SECRET_MANAGER_ROOT
     * |----- currentMasterKey
     * |----- nextMasterKey
     */
    private String zkRootNodePath;
    private String rmAppRoot;
    private String rmDTSecretManagerRoot;
    private String dtMasterKeysRootPath;
    private String delegationTokensRootPath;
    private String dtSequenceNumberPath;
    private String amrmTokenSecretManagerRoot;

    @VisibleForTesting
    protected String znodeWorkingPath;

    @VisibleForTesting
    protected ZooKeeper zkClient;

    /* activeZkClient is not used to do actual operations,
     * it is only used to verify client session for watched events and
     * it gets activated into zkClient on connection event.
     */
    @VisibleForTesting
    ZooKeeper activeZkClient;

    /**
     * Fencing related variables
     */
    private static final String FENCING_LOCK = "RM_ZK_FENCING_LOCK";
    private String fencingNodePath;
    private Op createFencingNodePathOp;
    private Op deleteFencingNodePathOp;
    private Thread verifyActiveStatusThread;
    private String zkRootNodeUsername;
    private final String zkRootNodePassword = Long.toString(random.nextLong());

    @VisibleForTesting
    List<ACL> zkRootNodeAcl;
    private boolean useDefaultFencingScheme = false;
    public static final int CREATE_DELETE_PERMS =
            ZooDefs.Perms.CREATE | ZooDefs.Perms.DELETE;
    private final String zkRootNodeAuthScheme =
            new DigestAuthenticationProvider().getScheme();

    /**
     * Given the {@link Configuration} and {@link ACL}s used (zkAcl) for
     * ZooKeeper access, construct the {@link ACL}s for the store's root node.
     * In the constructed {@link ACL}, all the users allowed by zkAcl are given
     * rwa access, while the current RM has exclude create-delete access.
     * <p>
     * To be called only when HA is enabled and the configuration doesn't set ACL
     * for the root node.
     */
    @VisibleForTesting
    @Private
    @Unstable
    protected List<ACL> constructZkRootNodeACL(
            Configuration conf, List<ACL> sourceACLs) throws NoSuchAlgorithmException {
        List<ACL> zkRootNodeAcl = new ArrayList<ACL>();
        for (ACL acl : sourceACLs) {
            zkRootNodeAcl.add(new ACL(
                    ZKUtil.removeSpecificPerms(acl.getPerms(), CREATE_DELETE_PERMS),
                    acl.getId()));
        }

        zkRootNodeUsername = HAUtil.getConfValueForRMInstance(
                YarnConfiguration.RM_ADDRESS,
                YarnConfiguration.DEFAULT_RM_ADDRESS, conf);
        Id rmId = new Id(zkRootNodeAuthScheme,
                DigestAuthenticationProvider.generateDigest(
                        zkRootNodeUsername + ":" + zkRootNodePassword));
        zkRootNodeAcl.add(new ACL(CREATE_DELETE_PERMS, rmId));
        return zkRootNodeAcl;
    }

    /**
     * 初始化ZK信息
     *
     * @param conf
     * @throws Exception
     */
    @Override
    public synchronized void initInternal(Configuration conf) throws Exception {
        zkHostPort = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
        if (zkHostPort == null) {
            throw new YarnRuntimeException("No server address specified for " +
                    "zookeeper state store for Resource Manager recovery. " +
                    YarnConfiguration.RM_ZK_ADDRESS + " is not configured.");
        }
        numRetries =
                conf.getInt(YarnConfiguration.RM_ZK_NUM_RETRIES,
                        YarnConfiguration.DEFAULT_ZK_RM_NUM_RETRIES); // ZK连接丢失时，重试连接ZK的次数
        znodeWorkingPath =
                conf.get(YarnConfiguration.ZK_RM_STATE_STORE_PARENT_PATH,
                        YarnConfiguration.DEFAULT_ZK_RM_STATE_STORE_PARENT_PATH); // 保存RM State的ZK路径
        zkSessionTimeout =
                conf.getInt(YarnConfiguration.RM_ZK_TIMEOUT_MS,
                        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS); // ZK session超时时间，以毫秒为单位

        // 计算重试连接ZK的时间间隔，以毫秒表示
        if (HAUtil.isHAEnabled(conf)) { // 高可用情况下是：重试时间间隔=session超时时间/重试ZK的次数
            zkRetryInterval = zkSessionTimeout / numRetries;
        } else {
            zkRetryInterval =
                    conf.getLong(YarnConfiguration.RM_ZK_RETRY_INTERVAL_MS,
                            YarnConfiguration.DEFAULT_RM_ZK_RETRY_INTERVAL_MS);
        }

        zkAcl = RMZKUtils.getZKAcls(conf);
        zkAuths = RMZKUtils.getZKAuths(conf);

        zkRootNodePath = getNodePath(znodeWorkingPath, ROOT_ZNODE_NAME);
        rmAppRoot = getNodePath(zkRootNodePath, RM_APP_ROOT);

        /* Initialize fencing related paths, acls, and ops */
        // 创建并删除fencingNodePath
        fencingNodePath = getNodePath(zkRootNodePath, FENCING_LOCK);
        createFencingNodePathOp = Op.create(fencingNodePath, new byte[0], zkAcl,
                CreateMode.PERSISTENT);
        deleteFencingNodePathOp = Op.delete(fencingNodePath, -1);
        if (HAUtil.isHAEnabled(conf)) {
            String zkRootNodeAclConf = HAUtil.getConfValueForRMInstance
                    (YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL, conf);
            if (zkRootNodeAclConf != null) {
                zkRootNodeAclConf = ZKUtil.resolveConfIndirection(zkRootNodeAclConf);
                try {
                    zkRootNodeAcl = ZKUtil.parseACLs(zkRootNodeAclConf);
                } catch (ZKUtil.BadAclFormatException bafe) {
                    LOG.error("Invalid format for " +
                            YarnConfiguration.ZK_RM_STATE_STORE_ROOT_NODE_ACL);
                    throw bafe;
                }
            } else {
                useDefaultFencingScheme = true;
                zkRootNodeAcl = constructZkRootNodeACL(conf, zkAcl);
            }
        }

        rmDTSecretManagerRoot =
                getNodePath(zkRootNodePath, RM_DT_SECRET_MANAGER_ROOT);
        dtMasterKeysRootPath = getNodePath(rmDTSecretManagerRoot,
                RM_DT_MASTER_KEYS_ROOT_ZNODE_NAME);
        delegationTokensRootPath = getNodePath(rmDTSecretManagerRoot,
                RM_DELEGATION_TOKENS_ROOT_ZNODE_NAME);
        dtSequenceNumberPath = getNodePath(rmDTSecretManagerRoot,
                RM_DT_SEQUENTIAL_NUMBER_ZNODE_NAME);
        amrmTokenSecretManagerRoot =
                getNodePath(zkRootNodePath, AMRMTOKEN_SECRET_MANAGER_ROOT);
    }

    /**
     * 连接ZK，创建ZK目录
     *
     * @throws Exception
     */
    @Override
    public synchronized void startInternal() throws Exception {
        // createConnection for future API calls
        createConnection();

        // ensure root dirs exist
        createRootDir(znodeWorkingPath); // 创建保存任务状态持久化节点
        createRootDir(zkRootNodePath);
        if (HAUtil.isHAEnabled(getConfig())) {
            fence();
            verifyActiveStatusThread = new VerifyActiveStatusThread();
            verifyActiveStatusThread.start();
        }
        createRootDir(rmAppRoot);
        createRootDir(rmDTSecretManagerRoot);
        createRootDir(dtMasterKeysRootPath);
        createRootDir(delegationTokensRootPath);
        createRootDir(dtSequenceNumberPath);
        createRootDir(amrmTokenSecretManagerRoot);
        syncInternal(zkRootNodePath);
    }

    private void createRootDir(final String rootPath) throws Exception {
        // For root dirs, we shouldn't use the doMulti helper methods
        new ZKAction<String>() {
            @Override
            public String run() throws KeeperException, InterruptedException {
                try {
                    return zkClient.create(rootPath, null, zkAcl, CreateMode.PERSISTENT);
                } catch (KeeperException ke) {
                    if (ke.code() == Code.NODEEXISTS) {
                        LOG.debug(rootPath + "znode already exists!");
                        return null;
                    } else {
                        throw ke;
                    }
                }
            }
        }.runWithRetries();
    }

    private void logRootNodeAcls(String prefix) throws Exception {
        Stat getStat = new Stat();
        List<ACL> getAcls = getACLWithRetries(zkRootNodePath, getStat);

        StringBuilder builder = new StringBuilder();
        builder.append(prefix);
        for (ACL acl : getAcls) {
            builder.append(acl.toString());
        }
        builder.append(getStat.toString());
        LOG.debug(builder.toString());
    }

    private synchronized void fence() throws Exception {
        if (LOG.isTraceEnabled()) {
            logRootNodeAcls("Before fencing\n");
        }

        new ZKAction<Void>() {
            @Override
            public Void run() throws KeeperException, InterruptedException {
                zkClient.setACL(zkRootNodePath, zkRootNodeAcl, -1);
                return null;
            }
        }.runWithRetries();

        // delete fencingnodepath
        new ZKAction<Void>() {
            @Override
            public Void run() throws KeeperException, InterruptedException {
                try {
                    zkClient.multi(Collections.singletonList(deleteFencingNodePathOp));
                } catch (KeeperException.NoNodeException nne) {
                    LOG.info("Fencing node " + fencingNodePath + " doesn't exist to delete");
                }
                return null;
            }
        }.runWithRetries();

        if (LOG.isTraceEnabled()) {
            logRootNodeAcls("After fencing\n");
        }
    }

    private synchronized void closeZkClients() throws IOException {
        zkClient = null;
        if (activeZkClient != null) {
            try {
                activeZkClient.close();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while closing ZK", e);
            }
            activeZkClient = null;
        }
    }

    @Override
    protected synchronized void closeInternal() throws Exception {
        if (verifyActiveStatusThread != null) {
            verifyActiveStatusThread.interrupt();
            verifyActiveStatusThread.join(1000);
        }
        closeZkClients();
    }

    @Override
    protected Version getCurrentVersion() {
        return CURRENT_VERSION_INFO;
    }

    @Override
    protected synchronized void storeVersion() throws Exception {
        String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);
        byte[] data =
                ((VersionPBImpl) CURRENT_VERSION_INFO).getProto().toByteArray();
        if (existsWithRetries(versionNodePath, true) != null) {
            setDataWithRetries(versionNodePath, data, -1);
        } else {
            createWithRetries(versionNodePath, data, zkAcl, CreateMode.PERSISTENT);
        }
    }

    @Override
    protected synchronized Version loadVersion() throws Exception {
        String versionNodePath = getNodePath(zkRootNodePath, VERSION_NODE);

        if (existsWithRetries(versionNodePath, true) != null) {
            byte[] data = getDataWithRetries(versionNodePath, true);
            Version version =
                    new VersionPBImpl(VersionProto.parseFrom(data));
            return version;
        }
        return null;
    }

    @Override
    public synchronized long getAndIncrementEpoch() throws Exception {
        String epochNodePath = getNodePath(zkRootNodePath, EPOCH_NODE);
        long currentEpoch = 0;
        if (existsWithRetries(epochNodePath, true) != null) {
            // load current epoch
            byte[] data = getDataWithRetries(epochNodePath, true);
            Epoch epoch = new EpochPBImpl(EpochProto.parseFrom(data));
            currentEpoch = epoch.getEpoch();
            // increment epoch and store it
            byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
                    .toByteArray();
            setDataWithRetries(epochNodePath, storeData, -1);
        } else {
            // initialize epoch node with 1 for the next time.
            byte[] storeData = Epoch.newInstance(currentEpoch + 1).getProto()
                    .toByteArray();
            createWithRetries(epochNodePath, storeData, zkAcl, CreateMode.PERSISTENT);
        }
        return currentEpoch;
    }

    @Override
    public synchronized RMState loadState() throws Exception {
        RMState rmState = new RMState();
        // recover DelegationTokenSecretManager
        loadRMDTSecretManagerState(rmState);
        // recover RM applications
        loadRMAppState(rmState);
        // recover AMRMTokenSecretManager
        loadAMRMTokenSecretManagerState(rmState);
        return rmState;
    }

    private void loadAMRMTokenSecretManagerState(RMState rmState)
            throws Exception {
        byte[] data = getDataWithRetries(amrmTokenSecretManagerRoot, true);
        if (data == null) {
            LOG.warn("There is no data saved");
            return;
        }
        AMRMTokenSecretManagerStatePBImpl stateData =
                new AMRMTokenSecretManagerStatePBImpl(
                        AMRMTokenSecretManagerStateProto.parseFrom(data));
        rmState.amrmTokenSecretManagerState =
                AMRMTokenSecretManagerState.newInstance(
                        stateData.getCurrentMasterKey(), stateData.getNextMasterKey());

    }

    private synchronized void loadRMDTSecretManagerState(RMState rmState)
            throws Exception {
        loadRMDelegationKeyState(rmState);
        loadRMSequentialNumberState(rmState);
        loadRMDelegationTokenState(rmState);
    }

    private void loadRMDelegationKeyState(RMState rmState) throws Exception {
        List<String> childNodes =
                getChildrenWithRetries(dtMasterKeysRootPath, true);
        for (String childNodeName : childNodes) {
            String childNodePath = getNodePath(dtMasterKeysRootPath, childNodeName);
            byte[] childData = getDataWithRetries(childNodePath, true);

            if (childData == null) {
                LOG.warn("Content of " + childNodePath + " is broken.");
                continue;
            }

            ByteArrayInputStream is = new ByteArrayInputStream(childData);
            DataInputStream fsIn = new DataInputStream(is);

            try {
                if (childNodeName.startsWith(DELEGATION_KEY_PREFIX)) {
                    DelegationKey key = new DelegationKey();
                    key.readFields(fsIn);
                    rmState.rmSecretManagerState.masterKeyState.add(key);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Loaded delegation key: keyId=" + key.getKeyId()
                                + ", expirationDate=" + key.getExpiryDate());
                    }
                }
            } finally {
                is.close();
            }
        }
    }

    private void loadRMSequentialNumberState(RMState rmState) throws Exception {
        byte[] seqData = getDataWithRetries(dtSequenceNumberPath, false);
        if (seqData != null) {
            ByteArrayInputStream seqIs = new ByteArrayInputStream(seqData);
            DataInputStream seqIn = new DataInputStream(seqIs);

            try {
                rmState.rmSecretManagerState.dtSequenceNumber = seqIn.readInt();
            } finally {
                seqIn.close();
            }
        }
    }

    private void loadRMDelegationTokenState(RMState rmState) throws Exception {
        List<String> childNodes =
                getChildrenWithRetries(delegationTokensRootPath, true);
        for (String childNodeName : childNodes) {
            String childNodePath =
                    getNodePath(delegationTokensRootPath, childNodeName);
            byte[] childData = getDataWithRetries(childNodePath, true);

            if (childData == null) {
                LOG.warn("Content of " + childNodePath + " is broken.");
                continue;
            }

            ByteArrayInputStream is = new ByteArrayInputStream(childData);
            DataInputStream fsIn = new DataInputStream(is);

            try {
                if (childNodeName.startsWith(DELEGATION_TOKEN_PREFIX)) {
                    RMDelegationTokenIdentifierData identifierData =
                            new RMDelegationTokenIdentifierData();
                    identifierData.readFields(fsIn);
                    RMDelegationTokenIdentifier identifier =
                            identifierData.getTokenIdentifier();
                    long renewDate = identifierData.getRenewDate();
                    rmState.rmSecretManagerState.delegationTokenState.put(identifier,
                            renewDate);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Loaded RMDelegationTokenIdentifier: " + identifier
                                + " renewDate=" + renewDate);
                    }
                }
            } finally {
                is.close();
            }
        }
    }

    private synchronized void loadRMAppState(RMState rmState) throws Exception {
        List<String> childNodes = getChildrenWithRetries(rmAppRoot, true);
        for (String childNodeName : childNodes) {
            String childNodePath = getNodePath(rmAppRoot, childNodeName);
            byte[] childData = getDataWithRetries(childNodePath, true);
            if (childNodeName.startsWith(ApplicationId.appIdStrPrefix)) {
                // application
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Loading application from znode: " + childNodeName);
                }
                ApplicationId appId = ConverterUtils.toApplicationId(childNodeName);
                ApplicationStateDataPBImpl appStateData =
                        new ApplicationStateDataPBImpl(
                                ApplicationStateDataProto.parseFrom(childData));
                ApplicationState appState =
                        new ApplicationState(appStateData.getSubmitTime(),
                                appStateData.getStartTime(),
                                appStateData.getApplicationSubmissionContext(),
                                appStateData.getUser(),
                                appStateData.getState(),
                                appStateData.getDiagnostics(), appStateData.getFinishTime());
                if (!appId.equals(appState.context.getApplicationId())) {
                    throw new YarnRuntimeException("The child node name is different " +
                            "from the application id");
                }
                rmState.appState.put(appId, appState);
                loadApplicationAttemptState(appState, appId);
            } else {
                LOG.info("Unknown child node with name: " + childNodeName);
            }
        }
    }

    private void loadApplicationAttemptState(ApplicationState appState,
                                             ApplicationId appId)
            throws Exception {
        String appPath = getNodePath(rmAppRoot, appId.toString());
        List<String> attempts = getChildrenWithRetries(appPath, false);
        for (String attemptIDStr : attempts) {
            if (attemptIDStr.startsWith(ApplicationAttemptId.appAttemptIdStrPrefix)) {
                String attemptPath = getNodePath(appPath, attemptIDStr);
                byte[] attemptData = getDataWithRetries(attemptPath, true);

                ApplicationAttemptId attemptId =
                        ConverterUtils.toApplicationAttemptId(attemptIDStr);
                ApplicationAttemptStateDataPBImpl attemptStateData =
                        new ApplicationAttemptStateDataPBImpl(
                                ApplicationAttemptStateDataProto.parseFrom(attemptData));
                Credentials credentials = null;
                if (attemptStateData.getAppAttemptTokens() != null) {
                    credentials = new Credentials();
                    DataInputByteBuffer dibb = new DataInputByteBuffer();
                    dibb.reset(attemptStateData.getAppAttemptTokens());
                    credentials.readTokenStorageStream(dibb);
                }

                ApplicationAttemptState attemptState =
                        new ApplicationAttemptState(attemptId,
                                attemptStateData.getMasterContainer(), credentials,
                                attemptStateData.getStartTime(), attemptStateData.getState(),
                                attemptStateData.getFinalTrackingUrl(),
                                attemptStateData.getDiagnostics(),
                                attemptStateData.getFinalApplicationStatus(),
                                attemptStateData.getAMContainerExitStatus(),
                                attemptStateData.getFinishTime(),
                                attemptStateData.getMemorySeconds(),
                                attemptStateData.getVcoreSeconds());


                appState.attempts.put(attemptState.getAttemptId(), attemptState);
            }
        }
        LOG.debug("Done loading applications from ZK state store");
    }

    /**
     * 存储任务状态
     *
     * @param appId
     * @param appStateDataPB
     * @throws Exception
     */
    @Override
    public synchronized void storeApplicationStateInternal(ApplicationId appId,
                                                           ApplicationStateData appStateDataPB) throws Exception {
        String nodeCreatePath = getNodePath(rmAppRoot, appId.toString());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Storing info for app: " + appId + " at: " + nodeCreatePath);
        }
        byte[] appStateData = appStateDataPB.getProto().toByteArray();
        createWithRetries(nodeCreatePath, appStateData, zkAcl,
                CreateMode.PERSISTENT);

    }

    /**
     * 更新任务状态
     *
     * @param appId
     * @param appStateDataPB
     * @throws Exception
     */
    @Override
    public synchronized void updateApplicationStateInternal(ApplicationId appId,
                                                            ApplicationStateData appStateDataPB) throws Exception {
        String nodeUpdatePath = getNodePath(rmAppRoot, appId.toString());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Storing final state info for app: " + appId + " at: "
                    + nodeUpdatePath);
        }

        // Application状态字节数组
        byte[] appStateData = appStateDataPB.getProto().toByteArray();

        // 调用重试逻辑写数据
        if (existsWithRetries(nodeUpdatePath, true) != null) {
            setDataWithRetries(nodeUpdatePath, appStateData, -1);
        } else {
            createWithRetries(nodeUpdatePath, appStateData, zkAcl,
                    CreateMode.PERSISTENT);
            LOG.debug(appId + " znode didn't exist. Created a new znode to"
                    + " update the application state.");
        }
    }

    /**
     * 存储任务重试信息
     *
     * @param appAttemptId
     * @param attemptStateDataPB
     * @throws Exception
     */
    @Override
    public synchronized void storeApplicationAttemptStateInternal(
            ApplicationAttemptId appAttemptId,
            ApplicationAttemptStateData attemptStateDataPB)
            throws Exception {
        String appDirPath = getNodePath(rmAppRoot,
                appAttemptId.getApplicationId().toString());
        String nodeCreatePath = getNodePath(appDirPath, appAttemptId.toString());

        if (LOG.isDebugEnabled()) {
            LOG.debug("Storing info for attempt: " + appAttemptId + " at: "
                    + nodeCreatePath);
        }
        byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();
        createWithRetries(nodeCreatePath, attemptStateData, zkAcl,
                CreateMode.PERSISTENT);
    }

    /**
     * 更新任务重试信息
     *
     * @param appAttemptId
     * @param attemptStateDataPB
     * @throws Exception
     */
    @Override
    public synchronized void updateApplicationAttemptStateInternal(
            ApplicationAttemptId appAttemptId,
            ApplicationAttemptStateData attemptStateDataPB)
            throws Exception {
        String appIdStr = appAttemptId.getApplicationId().toString();
        String appAttemptIdStr = appAttemptId.toString();
        String appDirPath = getNodePath(rmAppRoot, appIdStr);
        String nodeUpdatePath = getNodePath(appDirPath, appAttemptIdStr);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Storing final state info for attempt: " + appAttemptIdStr
                    + " at: " + nodeUpdatePath);
        }
        byte[] attemptStateData = attemptStateDataPB.getProto().toByteArray();

        if (existsWithRetries(nodeUpdatePath, true) != null) {
            setDataWithRetries(nodeUpdatePath, attemptStateData, -1);
        } else {
            createWithRetries(nodeUpdatePath, attemptStateData, zkAcl,
                    CreateMode.PERSISTENT);
            LOG.debug(appAttemptId + " znode didn't exist. Created a new znode to"
                    + " update the application attempt state.");
        }
    }


    /**
     * 删除application状态信息
     * @param appState
     * @throws Exception
     */
    @Override
    public synchronized void removeApplicationStateInternal(ApplicationState appState)
            throws Exception {
        String appId = appState.getAppId().toString(); // 获取应用ID
        String appIdRemovePath = getNodePath(rmAppRoot, appId); // 根据应用ID获取对应的路径
        ArrayList<Op> opList = new ArrayList<Op>();

        // 先执行application重试信息删除操作，后删除application状态信息
        for (ApplicationAttemptId attemptId : appState.attempts.keySet()) {
            String attemptRemovePath = getNodePath(appIdRemovePath, attemptId.toString());
            opList.add(Op.delete(attemptRemovePath, -1));
        }
        opList.add(Op.delete(appIdRemovePath, -1));

        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing info for app: " + appId + " at: " + appIdRemovePath
                    + " and its attempts.");
        }
        doMultiWithRetries(opList);
    }

    @Override
    protected synchronized void storeRMDelegationTokenAndSequenceNumberState(
            RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
            int latestSequenceNumber) throws Exception {
        ArrayList<Op> opList = new ArrayList<Op>();
        addStoreOrUpdateOps(
                opList, rmDTIdentifier, renewDate, latestSequenceNumber, false);
        doMultiWithRetries(opList);
    }

    @Override
    protected synchronized void removeRMDelegationTokenState(
            RMDelegationTokenIdentifier rmDTIdentifier) throws Exception {
        ArrayList<Op> opList = new ArrayList<Op>();
        String nodeRemovePath =
                getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
                        + rmDTIdentifier.getSequenceNumber());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing RMDelegationToken_"
                    + rmDTIdentifier.getSequenceNumber());
        }
        if (existsWithRetries(nodeRemovePath, true) != null) {
            opList.add(Op.delete(nodeRemovePath, -1));
        } else {
            LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
        }
        doMultiWithRetries(opList);
    }

    @Override
    protected void updateRMDelegationTokenAndSequenceNumberInternal(
            RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
            int latestSequenceNumber) throws Exception {
        ArrayList<Op> opList = new ArrayList<Op>();
        String nodeRemovePath =
                getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
                        + rmDTIdentifier.getSequenceNumber());
        if (existsWithRetries(nodeRemovePath, true) == null) {
            // in case znode doesn't exist
            addStoreOrUpdateOps(
                    opList, rmDTIdentifier, renewDate, latestSequenceNumber, false);
            LOG.debug("Attempted to update a non-existing znode " + nodeRemovePath);
        } else {
            // in case znode exists
            addStoreOrUpdateOps(
                    opList, rmDTIdentifier, renewDate, latestSequenceNumber, true);
        }
        doMultiWithRetries(opList);
    }

    private void addStoreOrUpdateOps(ArrayList<Op> opList,
                                     RMDelegationTokenIdentifier rmDTIdentifier, Long renewDate,
                                     int latestSequenceNumber, boolean isUpdate) throws Exception {
        // store RM delegation token
        String nodeCreatePath =
                getNodePath(delegationTokensRootPath, DELEGATION_TOKEN_PREFIX
                        + rmDTIdentifier.getSequenceNumber());
        ByteArrayOutputStream seqOs = new ByteArrayOutputStream();
        DataOutputStream seqOut = new DataOutputStream(seqOs);
        RMDelegationTokenIdentifierData identifierData =
                new RMDelegationTokenIdentifierData(rmDTIdentifier, renewDate);
        try {
            if (LOG.isDebugEnabled()) {
                LOG.debug((isUpdate ? "Storing " : "Updating ") + "RMDelegationToken_" +
                        rmDTIdentifier.getSequenceNumber());
            }

            if (isUpdate) {
                opList.add(Op.setData(nodeCreatePath, identifierData.toByteArray(), -1));
            } else {
                opList.add(Op.create(nodeCreatePath, identifierData.toByteArray(), zkAcl,
                        CreateMode.PERSISTENT));
            }


            seqOut.writeInt(latestSequenceNumber);
            if (LOG.isDebugEnabled()) {
                LOG.debug((isUpdate ? "Storing " : "Updating ") + dtSequenceNumberPath +
                        ". SequenceNumber: " + latestSequenceNumber);
            }

            opList.add(Op.setData(dtSequenceNumberPath, seqOs.toByteArray(), -1));
        } finally {
            seqOs.close();
        }
    }

    @Override
    protected synchronized void storeRMDTMasterKeyState(
            DelegationKey delegationKey) throws Exception {
        String nodeCreatePath =
                getNodePath(dtMasterKeysRootPath, DELEGATION_KEY_PREFIX
                        + delegationKey.getKeyId());
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        DataOutputStream fsOut = new DataOutputStream(os);
        if (LOG.isDebugEnabled()) {
            LOG.debug("Storing RMDelegationKey_" + delegationKey.getKeyId());
        }
        delegationKey.write(fsOut);
        try {
            createWithRetries(nodeCreatePath, os.toByteArray(), zkAcl,
                    CreateMode.PERSISTENT);
        } finally {
            os.close();
        }
    }

    @Override
    protected synchronized void removeRMDTMasterKeyState(
            DelegationKey delegationKey) throws Exception {
        String nodeRemovePath =
                getNodePath(dtMasterKeysRootPath, DELEGATION_KEY_PREFIX
                        + delegationKey.getKeyId());
        if (LOG.isDebugEnabled()) {
            LOG.debug("Removing RMDelegationKey_" + delegationKey.getKeyId());
        }
        if (existsWithRetries(nodeRemovePath, true) != null) {
            doMultiWithRetries(Op.delete(nodeRemovePath, -1));
        } else {
            LOG.debug("Attempted to delete a non-existing znode " + nodeRemovePath);
        }
    }

    @Override
    public synchronized void deleteStore() throws Exception {
        if (existsWithRetries(zkRootNodePath, true) != null) {
            deleteWithRetries(zkRootNodePath, true);
        }
    }

    // ZK related code

    /**
     * Watcher implementation which forward events to the ZKRMStateStore This
     * hides the ZK methods of the store from its public interface
     */
    private final class ForwardingWatcher implements Watcher {
        private ZooKeeper watchedZkClient;

        public ForwardingWatcher(ZooKeeper client) {
            this.watchedZkClient = client;
        }

        @Override
        public void process(WatchedEvent event) {
            try {
                ZKRMStateStore.this.processWatchEvent(watchedZkClient, event);
            } catch (Throwable t) {
                LOG.error("Failed to process watcher event " + event + ": "
                        + StringUtils.stringifyException(t));
            }
        }
    }

    @VisibleForTesting
    @Private
    @Unstable
    public synchronized void processWatchEvent(ZooKeeper zk,
                                               WatchedEvent event) throws Exception {
        // only process watcher event from current ZooKeeper Client session.
        if (zk != activeZkClient) {
            LOG.info("Ignore watcher event type: " + event.getType() +
                    " with state:" + event.getState() + " for path:" +
                    event.getPath() + " from old session");
            return;
        }

        Event.EventType eventType = event.getType();
        LOG.info("Watcher event type: " + eventType + " with state:"
                + event.getState() + " for path:" + event.getPath() + " for " + this);

        if (eventType == Event.EventType.None) {

            // the connection state has changed
            switch (event.getState()) {
                case SyncConnected:
                    LOG.info("ZKRMStateStore Session connected");
                    if (zkClient == null) {
                        // the SyncConnected must be from the client that sent Disconnected
                        zkClient = activeZkClient;
                        ZKRMStateStore.this.notifyAll();
                        LOG.info("ZKRMStateStore Session restored");
                    }
                    break;
                case Disconnected:
                    LOG.info("ZKRMStateStore Session disconnected");
                    zkClient = null;
                    break;
                case Expired:
                    // the connection got terminated because of session timeout
                    // call listener to reconnect
                    LOG.info("ZKRMStateStore Session expired");
                    createConnection();
                    syncInternal(event.getPath());
                    break;
                default:
                    LOG.error("Unexpected Zookeeper" +
                            " watch event state: " + event.getState());
                    break;
            }
        }
    }

    @VisibleForTesting
    @Private
    @Unstable
    String getNodePath(String root, String nodeName) {
        return (root + "/" + nodeName);
    }

    /**
     * Helper method to call ZK's sync() after calling createConnection().
     * Note that sync path is meaningless for now:
     * http://mail-archives.apache.org/mod_mbox/zookeeper-user/201102.mbox/browser
     *
     * @param path path to sync, nullable value. If the path is null,
     *             zkRootNodePath is used to sync.
     * @return true if ZK.sync() succeededs, false if ZK.sync() fails.
     * @throws InterruptedException
     */
    private void syncInternal(final String path) throws InterruptedException {
        final ZKSyncOperationCallback cb = new ZKSyncOperationCallback();
        final String pathForSync = (path != null) ? path : zkRootNodePath;
        try {
            new ZKAction<Void>() {
                @Override
                Void run() throws KeeperException, InterruptedException {
                    zkClient.sync(pathForSync, cb, null);
                    return null;
                }
            }.runWithRetries();
        } catch (Exception e) {
            LOG.fatal("sync failed.");
        }
    }

    /**
     * Helper method that creates fencing node, executes the passed operations,
     * and deletes the fencing node.
     */
    private synchronized void doMultiWithRetries(
            final List<Op> opList) throws Exception {
        final List<Op> execOpList = new ArrayList<Op>(opList.size() + 2);
        execOpList.add(createFencingNodePathOp);
        execOpList.addAll(opList);
        execOpList.add(deleteFencingNodePathOp);
        new ZKAction<Void>() {
            @Override
            public Void run() throws KeeperException, InterruptedException {
                zkClient.multi(execOpList);
                return null;
            }
        }.runWithRetries();
    }

    /**
     * Helper method that creates fencing node, executes the passed operation,
     * and deletes the fencing node.
     */
    private void doMultiWithRetries(final Op op) throws Exception {
        doMultiWithRetries(Collections.singletonList(op));
    }

    @VisibleForTesting
    @Private
    @Unstable
    public void createWithRetries(
            final String path, final byte[] data, final List<ACL> acl,
            final CreateMode mode) throws Exception {
        doMultiWithRetries(Op.create(path, data, acl, mode));
    }

    @VisibleForTesting
    @Private
    @Unstable
    public void setDataWithRetries(final String path, final byte[] data,
                                   final int version) throws Exception {
        doMultiWithRetries(Op.setData(path, data, version));
    }


    /**
     * 注册watch的方法
     */

    @VisibleForTesting
    @Private
    @Unstable
    public byte[] getDataWithRetries(final String path, final boolean watch)
            throws Exception {
        return new ZKAction<byte[]>() {
            @Override
            public byte[] run() throws KeeperException, InterruptedException {
                return zkClient.getData(path, watch, null);
            }
        }.runWithRetries();
    }

    private List<ACL> getACLWithRetries(
            final String path, final Stat stat) throws Exception {
        return new ZKAction<List<ACL>>() {
            @Override
            public List<ACL> run() throws KeeperException, InterruptedException {
                return zkClient.getACL(path, stat);
            }
        }.runWithRetries();
    }

    private List<String> getChildrenWithRetries(
            final String path, final boolean watch) throws Exception {
        return new ZKAction<List<String>>() {
            @Override
            List<String> run() throws KeeperException, InterruptedException {
                return zkClient.getChildren(path, watch);
            }
        }.runWithRetries();
    }

    private Stat existsWithRetries(
            final String path, final boolean watch) throws Exception {
        return new ZKAction<Stat>() {
            @Override
            Stat run() throws KeeperException, InterruptedException {
                return zkClient.exists(path, watch);
            }
        }.runWithRetries();
    }

    private void deleteWithRetries(
            final String path, final boolean watch) throws Exception {
        new ZKAction<Void>() {
            @Override
            Void run() throws KeeperException, InterruptedException {
                recursiveDeleteWithRetriesHelper(path, watch);
                return null;
            }
        }.runWithRetries();
    }

    /**
     * 注册watch的方法
     */

    /**
     * Helper method that deletes znodes recursively
     */
    private void recursiveDeleteWithRetriesHelper(String path, boolean watch)
            throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren(path, watch);
        for (String child : children) {
            recursiveDeleteWithRetriesHelper(path + "/" + child, false);
        }
        zkClient.delete(path, -1);
    }

    /**
     * Helper class that periodically attempts creating a znode to ensure that
     * this RM continues to be the Active.
     */
    private class VerifyActiveStatusThread extends Thread {
        private List<Op> emptyOpList = new ArrayList<Op>();

        VerifyActiveStatusThread() {
            super(VerifyActiveStatusThread.class.getName());
        }

        public void run() {
            try {
                while (true) {
                    doMultiWithRetries(emptyOpList);
                    Thread.sleep(zkSessionTimeout);
                }
            } catch (InterruptedException ie) {
                LOG.info(VerifyActiveStatusThread.class.getName() + " thread " +
                        "interrupted! Exiting!");
            } catch (Exception e) {
                notifyStoreOperationFailed(new StoreFencedException());
            }
        }
    }

    private abstract class ZKAction<T> {
        // run() expects synchronization on ZKRMStateStore.this
        abstract T run() throws KeeperException, InterruptedException;

        T runWithCheck() throws Exception {
            long startTime = System.currentTimeMillis();
            synchronized (ZKRMStateStore.this) {
                while (zkClient == null) {
                    ZKRMStateStore.this.wait(zkSessionTimeout);
                    if (zkClient != null) {
                        break;
                    }
                    if (System.currentTimeMillis() - startTime > zkSessionTimeout) {
                        throw new IOException("Wait for ZKClient creation timed out");
                    }
                }
                return run();
            }
        }

        private boolean shouldRetry(Code code) {
            switch (code) {
                case CONNECTIONLOSS: // 连接丢失
                case OPERATIONTIMEOUT: // 连接超时
                    return true;
                default:
                    break;
            }
            return false;
        }

        private boolean shouldRetryWithNewConnection(Code code) {
            // For fast recovery, we choose to close current connection after
            // SESSIONMOVED occurs. Latest state of a zknode path is ensured by
            // following zk.sync(path) operation.
            switch (code) {
                case SESSIONEXPIRED: // session超时
                case SESSIONMOVED: // session转移
                    return true;
                default:
                    break;
            }
            return false;
        }

        T runWithRetries() throws Exception {
            int retry = 0; // 保存重试次数
            while (true) {
                try {
                    return runWithCheck();
                } catch (KeeperException.NoAuthException nae) {
                    if (HAUtil.isHAEnabled(getConfig())) {
                        // NoAuthException possibly means that this store is fenced due to
                        // another RM becoming active. Even if not,
                        // it is safer to assume we have been fenced
                        throw new StoreFencedException();
                    }
                } catch (KeeperException ke) {
                    if (ke.code() == Code.NODEEXISTS) {
                        LOG.info("znode already exists!");
                        return null;
                    }
                    LOG.info("Exception while executing a ZK operation.", ke);
                    retry++; // 累计重试次数

                    /**
                     * 在未达到最大重试次数的条件的下，根据ZK Server返回的错误码执行不同的重试策略
                     */
                    if (shouldRetry(ke.code()) && retry < numRetries) { // 如果ZK Server返回的错误码为连接丢失、连接超时，则直接进行重试
                        LOG.info("Retrying operation on ZK. Retry no. " + retry);
                        Thread.sleep(zkRetryInterval);
                        continue;
                    }
                    if (shouldRetryWithNewConnection(ke.code()) && retry < numRetries) {  // 如果ZK Server返回的错误码为会话丢失、会话转移，则创建连接后进行重试
                        LOG.info("Retrying operation on ZK with new Connection. " +
                                "Retry no. " + retry);
                        Thread.sleep(zkRetryInterval);
                        createConnection();
                        syncInternal(ke.getPath());
                        continue;
                    }
                    LOG.info("Maxed out ZK retries. Giving up!"); // 达到最大重试次数，放弃重试，抛出异常
                    throw ke;
                }
            }
        }
    }

    private synchronized void createConnection()
            throws IOException, InterruptedException {
        closeZkClients();
        for (int retries = 0; retries < numRetries && zkClient == null;
             retries++) {
            try {
                activeZkClient = getNewZooKeeper();
                zkClient = activeZkClient;
                for (ZKUtil.ZKAuthInfo zkAuth : zkAuths) {
                    zkClient.addAuthInfo(zkAuth.getScheme(), zkAuth.getAuth());
                }
                if (useDefaultFencingScheme) {
                    zkClient.addAuthInfo(zkRootNodeAuthScheme,
                            (zkRootNodeUsername + ":" + zkRootNodePassword).getBytes());
                }
            } catch (IOException ioe) {
                // Retry in case of network failures
                LOG.info("Failed to connect to the ZooKeeper on attempt - " +
                        (retries + 1));
                ioe.printStackTrace();
            }
        }
        if (zkClient == null) {
            LOG.error("Unable to connect to Zookeeper");
            throw new YarnRuntimeException("Unable to connect to Zookeeper");
        }
        ZKRMStateStore.this.notifyAll();
        LOG.info("Created new ZK connection");
    }

    // protected to mock for testing
    @VisibleForTesting
    @Private
    @Unstable
    protected synchronized ZooKeeper getNewZooKeeper()
            throws IOException, InterruptedException {
        ZooKeeper zk = new ZooKeeper(zkHostPort, zkSessionTimeout, null);
        zk.register(new ForwardingWatcher(zk));
        return zk;
    }

    @Override
    public synchronized void storeOrUpdateAMRMTokenSecretManagerState(
            AMRMTokenSecretManagerState amrmTokenSecretManagerState,
            boolean isUpdate) {
        AMRMTokenSecretManagerState data =
                AMRMTokenSecretManagerState.newInstance(amrmTokenSecretManagerState);
        byte[] stateData = data.getProto().toByteArray();
        try {
            setDataWithRetries(amrmTokenSecretManagerRoot, stateData, -1);
        } catch (Exception ex) {
            LOG.info("Error storing info for AMRMTokenSecretManager", ex);
            notifyStoreOperationFailed(ex);
        }
    }

}
