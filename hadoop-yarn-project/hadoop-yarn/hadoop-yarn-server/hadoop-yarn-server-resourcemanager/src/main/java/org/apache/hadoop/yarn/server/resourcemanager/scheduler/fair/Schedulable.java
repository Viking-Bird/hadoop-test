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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

/**
 * A Schedulable represents an entity that can be scheduled such as an
 * application or a queue. It provides a common interface so that algorithms
 * such as fair sharing can be applied both within a queue and across queues.
 *
 * A Schedulable is responsible for three roles:
 * 1) Assign resources through {@link #assignContainer}.
 * 2) It provides information about the app/queue to the scheduler, including:
 *    - Demand (maximum number of tasks required)
 *    - Minimum share (for queues)
 *    - Job/queue weight (for fair sharing)
 *    - Start time and priority (for FIFO)
 * 3) It can be assigned a fair share, for use with fair scheduling.
 *
 * Schedulable also contains two methods for performing scheduling computations:
 * - updateDemand() is called periodically to compute the demand of the various
 *   jobs and queues, which may be expensive (e.g. jobs must iterate through all
 *   their tasks to count failed tasks, tasks that can be speculated, etc).
 * - redistributeShare() is called after demands are updated and a Schedulable's
 *   fair share has been set by its parent to let it distribute its share among
 *   the other Schedulables within it (e.g. for queues that want to perform fair
 *   sharing among their jobs).
 */
@Private
@Unstable
public interface Schedulable {
  /**
   * Name of job/queue, used for debugging as well as for breaking ties in
   * scheduling order deterministically.
   */
  public String getName();

  /**
   * Maximum number of resources required by this Schedulable. This is defined as
   * number of currently utilized resources + number of unlaunched resources (that
   * are either not yet launched or need to be speculated).
   * 当前的最大资源，包括已使用的资源和任务运行未获得的资源
   */
  public Resource getDemand();

  /** Get the aggregate amount of resources consumed by the schedulable. 已使用的总资源*/
  public Resource getResourceUsage();

  /** Minimum Resource share assigned to the schedulable. 已分配的最小资源 */
  public Resource getMinShare();

  /** Maximum Resource share assigned to the schedulable. 已分配的最大资源*/
  public Resource getMaxShare();

  /** Job/queue weight in fair sharing. */
  public ResourceWeights getWeights();

  /** Start time for jobs in FIFO queues; meaningless for QueueSchedulables.*/
  public long getStartTime();

 /** Job priority for jobs in FIFO queues; meaningless for QueueSchedulables. */
  public Priority getPriority();

  /** Refresh the Schedulable's demand and those of its children if any. */
  public void updateDemand();

  /**
   * Assign a container on this node if possible, and return the amount of
   * resources assigned. 给节点分配container，并返回已分配资源的数量
   */
  public Resource assignContainer(FSSchedulerNode node);

  /**
   * Preempt a container from this Schedulable if possible.
   */
  public RMContainer preemptContainer();

  /** Get the fair share assigned to this Schedulable. 返回调度对象的共享资源数量 */
  public Resource getFairShare();

  /** Assign a fair share to this Schedulable. 给调度对象分配共享资源*/
  public void setFairShare(Resource fairShare);
}
