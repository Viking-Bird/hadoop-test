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

package org.apache.hadoop.yarn.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Evolving;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.AbstractService;
import org.apache.hadoop.util.ShutdownHookManager;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnRuntimeException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Dispatches {@link Event}s in a separate thread. Currently only single thread
 * does that. Potentially there could be multiple channels for each event type
 * class and a thread pool can be used to dispatch the events.
 */
@SuppressWarnings("rawtypes")
@Public
@Evolving
public class AsyncDispatcher extends AbstractService implements Dispatcher {

    private static final Log LOG = LogFactory.getLog(AsyncDispatcher.class);

    // 存放事件的队列
    private final BlockingQueue<Event> eventQueue;
    // AsyncDispatcher是否停止的标志位
    private volatile boolean stopped = false;

    // Configuration flag for enabling/disabling draining dispatcher's events on
    // stop functionality.
    // 在stop功能中开启/禁用流尽分发器事件的配置标志位
    private volatile boolean drainEventsOnStop = false;

    // Indicates all the remaining dispatcher's events on stop have been drained
    // and processed.
    // stop功能中所有剩余分发器事件已经被处理或流尽的标志位
    private volatile boolean drained = true;

    // drained的等待锁
    private Object waitForDrained = new Object();

    // For drainEventsOnStop enabled only, block newly coming events into the
    // queue while stopping.
    // 在AsyncDispatcher停止过程中阻塞新近到来的事件进入队列的标志位，仅当drainEventsOnStop启用（即为true）时有效
    private volatile boolean blockNewEvents = false;
    // 事件处理器实例，负责将新事件放入队列
    private EventHandler handlerInstance = null;

    // 事件处理调度线程
    private Thread eventHandlingThread;

    // 事件类型枚举类Enum到事件处理器EventHandler实例的映射集合
    protected final Map<Class<? extends Enum>, EventHandler> eventDispatchers;

    // 标志位：确保调度程序崩溃，但不做系统退出system-exit
    private boolean exitOnDispatchException;

    public AsyncDispatcher() {
        this(new LinkedBlockingQueue<Event>());
    }

    public AsyncDispatcher(BlockingQueue<Event> eventQueue) {
        super("Dispatcher");
        this.eventQueue = eventQueue;
        this.eventDispatchers = new HashMap<Class<? extends Enum>, EventHandler>();
    }

    Runnable createThread() {
        return new Runnable() {
            @Override
            public void run() {
                while (!stopped && !Thread.currentThread().isInterrupted()) {
                    drained = eventQueue.isEmpty();
                    // blockNewEvents is only set when dispatcher is draining to stop,
                    // adding this check is to avoid the overhead of acquiring the lock
                    // and calling notify every time in the normal run of the loop.
                    // 如果blockNewEvents=true，代表该调度器目前正在进行关闭操作，因此，事件处理线程有必要在发现事件队列已经清空的情况下，唤醒服务关闭线程，执行调度器关闭操作
                    if (blockNewEvents) {
                        synchronized (waitForDrained) {
                            if (drained) {
                                // 通过waitForDrained锁及时通知到服务关闭线程(waitForDrained.notify())，服务关闭线程收到该通知就能立刻从waitForDrained.wait()中直接唤醒，从而及时完成关闭操作，而不再进行不必要的wait操作
                                waitForDrained.notify();
                            }
                        }
                    }
                    // 取出一个事件，调用dispatch方法进行处理
                    Event event;
                    try {
                        event = eventQueue.take();
                    } catch (InterruptedException ie) {
                        if (!stopped) {
                            LOG.warn("AsyncDispatcher thread interrupted", ie);
                        }
                        return;
                    }
                    if (event != null) {
                        dispatch(event);
                    }
                }
            }
        };
    }

    @Override
    protected void serviceInit(Configuration conf) throws Exception {
        // 标识遇到错误是否退出
        this.exitOnDispatchException =
                conf.getBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY,
                        Dispatcher.DEFAULT_DISPATCHER_EXIT_ON_ERROR);
        super.serviceInit(conf);
    }

    @Override
    protected void serviceStart() throws Exception {
        //start all the components
        super.serviceStart();
        eventHandlingThread = new Thread(createThread());
        eventHandlingThread.setName("AsyncDispatcher event handler");
        eventHandlingThread.start();
    }

    public void setDrainEventsOnStop() {
        drainEventsOnStop = true;
    }

    @Override
    protected void serviceStop() throws Exception {
        if (drainEventsOnStop) {
            blockNewEvents = true;  //首先阻止新任务的分派，试图优雅停掉当前线程的工作
            LOG.info("AsyncDispatcher is draining to stop, igonring any new events.");
            long endTime = System.currentTimeMillis() + getConfig()
                    .getLong(YarnConfiguration.DISPATCHER_DRAIN_EVENTS_TIMEOUT,
                            YarnConfiguration.DEFAULT_DISPATCHER_DRAIN_EVENTS_TIMEOUT);

            // 服务关闭的时候会每1s检查我们的eventQueue中的所有事件是否处理完毕，如果没有处理完毕，则继续等待。
            synchronized (waitForDrained) {
                while (!drained && eventHandlingThread != null
                        && eventHandlingThread.isAlive()
                        && System.currentTimeMillis() < endTime) {
                    waitForDrained.wait(1000); // 代表着服务关闭的时候会每1s检查我们的eventQueue中的所有事件是否处理完毕，如果没有处理完毕，则继续等待
                    LOG.info("Waiting for AsyncDispatcher to drain.");
                }
            }
        }
        stopped = true;
        if (eventHandlingThread != null) {
            eventHandlingThread.interrupt(); //防止线程正在处理一个耗时任务导致线程依然没有退出
            try {
                eventHandlingThread.join(); //等待eventHandlingThread执行完毕
            } catch (InterruptedException ie) {
                LOG.warn("Interrupted Exception while stopping", ie);
            }
        }

        // stop all the components
        super.serviceStop();
    }

    @SuppressWarnings("unchecked")
    protected void dispatch(Event event) {
        //all events go thru this loop
        if (LOG.isDebugEnabled()) {
            LOG.debug("Dispatching the event " + event.getClass().getName() + "."
                    + event.toString());
        }

        // 获取事件类型对应的类
        Class<? extends Enum> type = event.getType().getDeclaringClass();

        try {
            // 获取事件处理器
            EventHandler handler = eventDispatchers.get(type);
            if (handler != null) {
                handler.handle(event);
            } else {
                throw new Exception("No handler for registered for " + type);
            }
        } catch (Throwable t) {
            //TODO Maybe log the state of the queue
            LOG.fatal("Error in dispatcher thread", t);
            // If serviceStop is called, we should exit this thread gracefully.
            // 如果事件处理过程中遇到异常，判断是否开启了遇到异常就退出开关，则进行退出操作
            if (exitOnDispatchException
                    && (ShutdownHookManager.get().isShutdownInProgress()) == false
                    && stopped == false) {
                Thread shutDownThread = new Thread(createShutDownThread());
                shutDownThread.setName("AsyncDispatcher ShutDown handler");
                shutDownThread.start();
            }
        }
    }

    /**
     * 注册事件分派器
     * @param eventType
     * @param handler
     */
    @SuppressWarnings("unchecked")
    @Override
    public void register(Class<? extends Enum> eventType,
                         EventHandler handler) {
        /* check to see if we have a listener registered */
        EventHandler<Event> registeredHandler = (EventHandler<Event>)
                eventDispatchers.get(eventType);
        LOG.info("Registering " + eventType + " for " + handler.getClass());
        if (registeredHandler == null) { // 如果没有注册过，直接放入eventDispatchers进行注册
            eventDispatchers.put(eventType, handler);
        } else if (!(registeredHandler instanceof MultiListenerHandler)) {
            /* for multiple listeners of an event add the multiple listener handler */
            // 如果已经注册过，且不是MultiListenerHandler：构造一个MultiListenerHandler实例，将之前注册过的事件处理器registeredHandler连同这次需要注册的事件处理器handler，做为一个多路复合监听事件处理器注册到eventDispatchers
            MultiListenerHandler multiHandler = new MultiListenerHandler();
            multiHandler.addHandler(registeredHandler);
            multiHandler.addHandler(handler);
            eventDispatchers.put(eventType, multiHandler);
        } else {
            /* already a multilistener, just add to it */
            // 如果已经注册过，且是MultiListenerHandler：强制转换下，直接追加注册新的handler。
            MultiListenerHandler multiHandler
                    = (MultiListenerHandler) registeredHandler;
            multiHandler.addHandler(handler);
        }
    }

    @Override
    public EventHandler getEventHandler() {
        if (handlerInstance == null) {
            handlerInstance = new GenericEventHandler();
        }
        return handlerInstance;
    }

    /**
     * 负责将新事件添加到队列中
     */
    class GenericEventHandler implements EventHandler<Event> {

        public void handle(Event event) {
            if (blockNewEvents) {
                return;
            }
            drained = false;

            /* all this method does is enqueue all the events onto the queue */
            // eventQueue size不为0且是1000的整数倍则打印一条log
            int qSize = eventQueue.size();
            if (qSize != 0 && qSize % 1000 == 0) {
                LOG.info("Size of event-queue is " + qSize);
            }
            int remCapacity = eventQueue.remainingCapacity();
            // 剩余的空间小于1000时打印一个warn log
            if (remCapacity < 1000) {
                LOG.warn("Very low remaining capacity in the event-queue: "
                        + remCapacity);
            }
            try {
                eventQueue.put(event);
            } catch (InterruptedException e) {
                if (!stopped) {
                    LOG.warn("AsyncDispatcher thread interrupted", e);
                }
                // Need to reset drained flag to true if event queue is empty,
                // otherwise dispatcher will hang on stop.
                drained = eventQueue.isEmpty();
                throw new YarnRuntimeException(e);
            }
        }
    }

    @VisibleForTesting
    protected boolean isEventThreadWaiting() {
        return eventHandlingThread.getState() == Thread.State.WAITING;
    }

    /**
     * 多路复合监听事件处理器
     * Multiplexing an event. Sending it to different handlers that
     * are interested in the event.
     * @param <T> the type of event these multiple handlers are interested in.
     */
    static class MultiListenerHandler implements EventHandler<Event> {
        List<EventHandler<Event>> listofHandlers;

        public MultiListenerHandler() {
            listofHandlers = new ArrayList<EventHandler<Event>>();
        }

        @Override
        public void handle(Event event) {
            for (EventHandler<Event> handler : listofHandlers) {
                handler.handle(event);
            }
        }

        void addHandler(EventHandler<Event> handler) {
            listofHandlers.add(handler);
        }

    }

    Runnable createShutDownThread() {
        return new Runnable() {
            @Override
            public void run() {
                LOG.info("Exiting, bbye..");
                System.exit(-1);
            }
        };
    }
}
