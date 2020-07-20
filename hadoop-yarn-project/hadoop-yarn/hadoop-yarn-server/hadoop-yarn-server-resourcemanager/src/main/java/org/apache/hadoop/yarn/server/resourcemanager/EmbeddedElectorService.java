/**
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.yarn.server.resourcemanager;

import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.ha.ActiveStandbyElector;
import org.apache.hadoop.ha.HAServiceProtocol;
import org.apache.hadoop.ha.ServiceFailedException;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ZKUtil;
import org.apache.hadoop.yarn.conf.HAUtil;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;
import org.apache.hadoop.yarn.proto.YarnServerResourceManagerServiceProtos;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.ACL;

import java.io.IOException;
import java.util.List;

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmbeddedElectorService extends AbstractService
    implements ActiveStandbyElector.ActiveStandbyElectorCallback {
  private static final Log LOG =
      LogFactory.getLog(EmbeddedElectorService.class.getName());
  private static final HAServiceProtocol.StateChangeRequestInfo req =
      new HAServiceProtocol.StateChangeRequestInfo(
          HAServiceProtocol.RequestSource.REQUEST_BY_ZKFC);

  private RMContext rmContext;

  private byte[] localActiveNodeInfo;
  private ActiveStandbyElector elector;

  EmbeddedElectorService(RMContext rmContext) {
    super(EmbeddedElectorService.class.getName());
    this.rmContext = rmContext;
  }

  @Override
  protected void serviceInit(Configuration conf)
      throws Exception {
    conf = conf instanceof YarnConfiguration ? conf : new YarnConfiguration(conf);

    // 读取ZK集群地址
    String zkQuorum = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
    if (zkQuorum == null) {
     throw new YarnRuntimeException("Embedded automatic failover " +
          "is enabled, but " + YarnConfiguration.RM_ZK_ADDRESS +
          " is not set");
    }

    // 读取yarn.resourcemanager.ha.id属性，通常主备机以rm1和rm2命名
    String rmId = HAUtil.getRMHAId(conf);
    // 读取集群ID属性，也就是yarn.resourcemanager.cluster-id的值
    String clusterId = YarnConfiguration.getClusterId(conf);
    localActiveNodeInfo = createActiveNodeInfo(clusterId, rmId);

    // 设置YARN选举的父ZNode，默认值是/yarn-leader-election
    String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
        YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
    // 设置选举的地址：/yarn-leader-election+clusterId
    String electionZNode = zkBasePath + "/" + clusterId;

    // ZooKeeper session超时时间
    long zkSessionTimeout = conf.getLong(YarnConfiguration.RM_ZK_TIMEOUT_MS,
        YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

    List<ACL> zkAcls = RMZKUtils.getZKAcls(conf);
    List<ZKUtil.ZKAuthInfo> zkAuths = RMZKUtils.getZKAuths(conf);

    // 设置ActiveStandbyElector选举操作重试的最大次数，默认是3
    int maxRetryNum = conf.getInt(
        CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY,
        CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT);
    // 创建ActiveStandbyElector对象
    elector = new ActiveStandbyElector(zkQuorum, (int) zkSessionTimeout,
        electionZNode, zkAcls, zkAuths, this, maxRetryNum);

    elector.ensureParentZNode();
    if (!isParentZnodeSafe(clusterId)) {
      notifyFatalError(electionZNode + " znode has invalid data! "+
          "Might need formatting!");
    }

    super.serviceInit(conf);
  }

  @Override
  protected void serviceStart() throws Exception {
    elector.joinElection(localActiveNodeInfo);
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception {
    /**
     * When error occurs in serviceInit(), serviceStop() can be called.
     * We need null check for the case.
     */
    if (elector != null) {
      elector.quitElection(false);
      elector.terminateConnection();
    }
    super.serviceStop();
  }

  @Override
  public void becomeActive() throws ServiceFailedException {
    try {
      rmContext.getRMAdminService().transitionToActive(req);
    } catch (Exception e) {
      throw new ServiceFailedException("RM could not transition to Active", e);
    }
  }

  @Override
  public void becomeStandby() {
    try {
      rmContext.getRMAdminService().transitionToStandby(req);
    } catch (Exception e) {
      LOG.error("RM could not transition to Standby", e);
    }
  }

  @Override
  public void enterNeutralMode() {
    /**
     * Possibly due to transient connection issues. Do nothing.
     * TODO: Might want to keep track of how long in this state and transition
     * to standby.
     */
  }

  @SuppressWarnings(value = "unchecked")
  @Override
  public void notifyFatalError(String errorMessage) {
    rmContext.getDispatcher().getEventHandler().handle(
        new RMFatalEvent(RMFatalEventType.EMBEDDED_ELECTOR_FAILED, errorMessage));
  }

  @Override
  public void fenceOldActive(byte[] oldActiveData) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Request to fence old active being ignored, " +
          "as embedded leader election doesn't support fencing");
    }
  }

  private static byte[] createActiveNodeInfo(String clusterId, String rmId)
      throws IOException {
    return YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
        .newBuilder()
        .setClusterId(clusterId)
        .setRmId(rmId)
        .build()
        .toByteArray();
  }

  /**
   * 对保存选举锁节点的父节点进行安全性检查：1、检查父节点是否已经在ZK中存在；2、如果存在，检查clusterId是否一致，不一致，则记录异常说明存在另外一个集群的选举信息
   * @param clusterId
   * @return
   * @throws InterruptedException
   * @throws IOException
   * @throws KeeperException
   */
  private boolean isParentZnodeSafe(String clusterId)
      throws InterruptedException, IOException, KeeperException {
    byte[] data;
    try {
      data = elector.getActiveData();
    } catch (ActiveStandbyElector.ActiveNotFoundException e) {
      // no active found, parent znode is safe
      return true;
    }

    YarnServerResourceManagerServiceProtos.ActiveRMInfoProto proto;
    try {
      proto = YarnServerResourceManagerServiceProtos.ActiveRMInfoProto
          .parseFrom(data);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Invalid data in ZK: " + StringUtils.byteToHexString(data));
      return false;
    }

    // Check if the passed proto corresponds to an RM in the same cluster
    if (!proto.getClusterId().equals(clusterId)) {
      LOG.error("Mismatched cluster! The other RM seems " +
          "to be from a different cluster. Current cluster = " + clusterId +
          "Other RM's cluster = " + proto.getClusterId());
      return false;
    }
    return true;
  }

  public void resetLeaderElection() {
    elector.quitElection(false);
    elector.joinElection(localActiveNodeInfo);
  }

  public String getHAZookeeperConnectionState() {
    return elector.getHAZookeeperConnectionState();
  }
}
