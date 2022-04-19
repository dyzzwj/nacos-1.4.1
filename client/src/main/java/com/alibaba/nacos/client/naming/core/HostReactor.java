/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.client.naming.core;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.beat.BeatInfo;
import com.alibaba.nacos.client.naming.beat.BeatReactor;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.client.naming.event.InstancesChangeNotifier;
import com.alibaba.nacos.client.naming.net.NamingProxy;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Host reactor.
 *
 * @author xuanyin
 */
public class HostReactor implements Closeable {

    private static final long DEFAULT_DELAY = 1000L;

    private static final long UPDATE_HOLD_INTERVAL = 5000L;

    //服务更新任务 存储服务唯一标识与服务更新Future的映射关系
    private final Map<String, ScheduledFuture<?>> futureMap = new HashMap<String, ScheduledFuture<?>>();

    //服务注册表 服务唯一标识与服务信息ServiceInfo的映射关系。服务唯一标识=groupName+@@+serviceName+@@+clusterName，其中clusterName部分可以为空
    private final Map<String, ServiceInfo> serviceInfoMap;

    //服务更新表 存储正在执行更新操作的service唯一标识。
    private final Map<String, Object> updatingMap;

    private final PushReceiver pushReceiver;

    private final BeatReactor beatReactor;

    private final NamingProxy serverProxy;

    //failover处理器
    private final FailoverReactor failoverReactor;

    private final String cacheDir;

    private final boolean pushEmptyProtection;

    private final ScheduledExecutorService executor;

    private final InstancesChangeNotifier notifier;

    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir) {
        this(serverProxy, beatReactor, cacheDir, false, false, UtilAndComs.DEFAULT_POLLING_THREAD_COUNT);
    }

    public HostReactor(NamingProxy serverProxy, BeatReactor beatReactor, String cacheDir, boolean loadCacheAtStart,
            boolean pushEmptyProtection, int pollingThreadCount) {
        // init executorService
        this.executor = new ScheduledThreadPoolExecutor(pollingThreadCount, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                thread.setName("com.alibaba.nacos.client.naming.updater");
                return thread;
            }
        });

        this.beatReactor = beatReactor;
        this.serverProxy = serverProxy;
        this.cacheDir = cacheDir;
        if (loadCacheAtStart) {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<String, ServiceInfo>(16);
        }
        this.pushEmptyProtection = pushEmptyProtection;
        this.updatingMap = new ConcurrentHashMap<String, Object>();
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        //负责处理服务端推送ServiceInfo信息，基于UDP协议
        this.pushReceiver = new PushReceiver(this);
        this.notifier = new InstancesChangeNotifier();

        NotifyCenter.registerToPublisher(InstancesChangeEvent.class, 16384);
        NotifyCenter.registerSubscriber(notifier);
    }

    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }

    public synchronized ScheduledFuture<?> addTask(UpdateTask task) {
        return executor.schedule(task, DEFAULT_DELAY, TimeUnit.MILLISECONDS);
    }

    /**
     * subscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void subscribe(String serviceName, String clusters, EventListener eventListener) {
        // 1. InstancesChangeNotifier注册监听器
        notifier.registerListener(serviceName, clusters, eventListener);
        //2.确保初始化注册表信息并开启定时任务拉取注册表。
        getServiceInfo(serviceName, clusters);
    }

    /**
     * unsubscribe instancesChangeEvent.
     *
     * @param serviceName   combineServiceName, such as 'xxx@@xxx'
     * @param clusters      clusters, concat by ','. such as 'xxx,yyy'
     * @param eventListener custom listener
     */
    public void unSubscribe(String serviceName, String clusters, EventListener eventListener) {
        notifier.deregisterListener(serviceName, clusters, eventListener);
    }

    public List<ServiceInfo> getSubscribeServices() {
        return notifier.getSubscribeServices();
    }

    /**
     * Process service json.
     *
     * @param json service json
     * @return service info
     */
    public ServiceInfo processServiceJson(String json) {
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());

        if (pushEmptyProtection && !serviceInfo.validate()) {
            //empty or error push, just ignore
            return oldService;
        }

        boolean changed = false;
        // 如果已经存在ServiceInfo，比较新老ServiceInfo
        if (oldService != null) {

            if (oldService.getLastRefTime() > serviceInfo.getLastRefTime()) {
                NAMING_LOGGER.warn("out of date data received, old-t: " + oldService.getLastRefTime() + ", new-t: "
                        + serviceInfo.getLastRefTime());
            }

            //更新注册表
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);

            Map<String, Instance> oldHostMap = new HashMap<String, Instance>(oldService.getHosts().size());
            for (Instance host : oldService.getHosts()) {
                oldHostMap.put(host.toInetAddr(), host);
            }

            Map<String, Instance> newHostMap = new HashMap<String, Instance>(serviceInfo.getHosts().size());
            for (Instance host : serviceInfo.getHosts()) {
                newHostMap.put(host.toInetAddr(), host);
            }

            Set<Instance> modHosts = new HashSet<Instance>();
            Set<Instance> newHosts = new HashSet<Instance>();
            Set<Instance> remvHosts = new HashSet<Instance>();

            List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<Map.Entry<String, Instance>>(
                    newHostMap.entrySet());
            for (Map.Entry<String, Instance> entry : newServiceHosts) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (oldHostMap.containsKey(key) && !StringUtils
                        .equals(host.toString(), oldHostMap.get(key).toString())) {
                    modHosts.add(host);
                    continue;
                }

                if (!oldHostMap.containsKey(key)) {
                    newHosts.add(host);
                }
            }

            for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
                Instance host = entry.getValue();
                String key = entry.getKey();
                if (newHostMap.containsKey(key)) {
                    continue;
                }

                if (!newHostMap.containsKey(key)) {
                    remvHosts.add(host);
                }

            }

            if (newHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("new ips(" + newHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(newHosts));
            }

            if (remvHosts.size() > 0) {
                changed = true;
                NAMING_LOGGER.info("removed ips(" + remvHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(remvHosts));
            }

            if (modHosts.size() > 0) {
                changed = true;
                // 如果存在instance对应心跳任务，更新对应心跳任务的BeatInfo信息
                updateBeatInfo(modHosts);
                NAMING_LOGGER.info("modified ips(" + modHosts.size() + ") service: " + serviceInfo.getKey() + " -> "
                        + JacksonUtils.toJson(modHosts));
            }

            serviceInfo.setJsonFromServer(json);

            if (newHosts.size() > 0 || remvHosts.size() > 0 || modHosts.size() > 0) {
                // 发布InstancesChangeEvent事件，将serviceInfo写入本地磁盘用作failover
                NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                        serviceInfo.getClusters(), serviceInfo.getHosts()));
                DiskCache.write(serviceInfo, cacheDir);
            }

        } else {
            // 如果不存在老的ServiceInfo
            changed = true;
            NAMING_LOGGER.info("init new ips(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
            // 更新注册表，发布InstancesChangeEvent事件，将serviceInfo写入本地磁盘用作failover
            //InstancesChangeNotifier.onEvent处理InstancesChangeEvent事件
            serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
            NotifyCenter.publishEvent(new InstancesChangeEvent(serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            serviceInfo.setJsonFromServer(json);
            DiskCache.write(serviceInfo, cacheDir);
        }

        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());

        if (changed) {
            NAMING_LOGGER.info("current ips:(" + serviceInfo.ipCount() + ") service: " + serviceInfo.getKey() + " -> "
                    + JacksonUtils.toJson(serviceInfo.getHosts()));
        }

        return serviceInfo;
    }

    private void updateBeatInfo(Set<Instance> modHosts) {
        for (Instance instance : modHosts) {
            String key = beatReactor.buildKey(instance.getServiceName(), instance.getIp(), instance.getPort());
            if (beatReactor.dom2Beat.containsKey(key) && instance.isEphemeral()) {
                BeatInfo beatInfo = beatReactor.buildBeatInfo(instance);
                beatReactor.addBeatInfo(instance.getServiceName(), beatInfo);
            }
        }
    }

    private ServiceInfo getServiceInfo0(String serviceName, String clusters) {

        String key = ServiceInfo.getKey(serviceName, clusters);

        return serviceInfoMap.get(key);
    }

    public ServiceInfo getServiceInfoDirectlyFromServer(final String serviceName, final String clusters)
            throws NacosException {
        String result = serverProxy.queryList(serviceName, clusters, 0, false);
        if (StringUtils.isNotEmpty(result)) {
            return JacksonUtils.toObj(result, ServiceInfo.class);
        }
        return null;
    }

    public ServiceInfo getServiceInfo(final String serviceName, final String clusters) {

        NAMING_LOGGER.debug("failover-mode: " + failoverReactor.isFailoverSwitch());
        String key = ServiceInfo.getKey(serviceName, clusters);
        // 1. 优先判断当前是否处于failover状态---由本地配置文件决定
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }
        // 2. 内存serviceInfoMap中查询ServiceInfo
        ServiceInfo serviceObj = getServiceInfo0(serviceName, clusters);

        if (null == serviceObj) {
            // 2-1. 内存中不存在，向服务端发起查询
            serviceObj = new ServiceInfo(serviceName, clusters);

            serviceInfoMap.put(serviceObj.getKey(), serviceObj);

            updatingMap.put(serviceName, new Object());
            //向服务端发起查询，并更新注册表
            updateServiceNow(serviceName, clusters);
            updatingMap.remove(serviceName);

        } else if (updatingMap.containsKey(serviceName)) {
            // 2-2. 内存中存在，如果正在更新注册表里的这个service，等待更新
            if (UPDATE_HOLD_INTERVAL > 0) {
                // hold a moment waiting for update finish
                synchronized (serviceObj) {
                    try {
                        serviceObj.wait(UPDATE_HOLD_INTERVAL);
                    } catch (InterruptedException e) {
                        NAMING_LOGGER
                                .error("[getServiceInfo] serviceName:" + serviceName + ", clusters:" + clusters, e);
                    }
                }
            }
        }
        // 3. 提交一个任务，定时更新serviceInfo 当查询时futureMap中不存在对应service的更新任务Future，提交一个任务用于更新service对应注册表。
        scheduleUpdateIfAbsent(serviceName, clusters);
        // 4. 返回内存map中的serviceInfo
        return serviceInfoMap.get(serviceObj.getKey());
    }

    private void updateServiceNow(String serviceName, String clusters) {
        try {
            updateService(serviceName, clusters);
        } catch (NacosException e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    /**
     * Schedule update if absent.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void scheduleUpdateIfAbsent(String serviceName, String clusters) {
        //当查询时futureMap中不存在对应service的更新任务Future，提交一个任务用于更新service对应注册表。
        if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
            return;
        }

        synchronized (futureMap) {
            if (futureMap.get(ServiceInfo.getKey(serviceName, clusters)) != null) {
                return;
            }

            ScheduledFuture<?> future = addTask(new UpdateTask(serviceName, clusters));
            futureMap.put(ServiceInfo.getKey(serviceName, clusters), future);
        }
    }

    /**
     * Update service now.
     *
     * @param serviceName service name
     * @param clusters    clusters
     */
    public void updateService(String serviceName, String clusters) throws NacosException {
        // 1. 获取老的ServiceInfo
        ServiceInfo oldService = getServiceInfo0(serviceName, clusters);
        try {
            // 2. GET /nacos/v1/ns/instance/list    pushReceiver.getUdpPort() 携带了udpPort
            String result = serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);
            // 3. 更新注册表
            if (StringUtils.isNotEmpty(result)) {
                processServiceJson(result);
            }
        } finally {
            if (oldService != null) {
                // 4. 通知等待在老serviceInfo上的线程（由于updatingMap中存在service而等待）
                synchronized (oldService) {
                    oldService.notifyAll();
                }
            }
        }
    }

    /**
     * Refresh only.
     *
     * @param serviceName service name
     * @param clusters    cluster
     */
    public void refreshOnly(String serviceName, String clusters) {
        try {
            serverProxy.queryList(serviceName, clusters, pushReceiver.getUdpPort(), false);
        } catch (Exception e) {
            NAMING_LOGGER.error("[NA] failed to update serviceName: " + serviceName, e);
        }
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executor, NAMING_LOGGER);
        pushReceiver.shutdown();
        failoverReactor.shutdown();
        NotifyCenter.deregisterSubscriber(notifier);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    public class UpdateTask implements Runnable {

        long lastRefTime = Long.MAX_VALUE;

        private final String clusters;

        private final String serviceName;

        /**
         * the fail situation. 1:can't connect to server 2:serviceInfo's hosts is empty
         */
        private int failCount = 0;

        public UpdateTask(String serviceName, String clusters) {
            this.serviceName = serviceName;
            this.clusters = clusters;
        }

        private void incFailCount() {
            int limit = 6;
            if (failCount == limit) {
                return;
            }
            failCount++;
        }

        private void resetFailCount() {
            failCount = 0;
        }

        @Override
        public void run() {
            // 下次拉取服务注册表的延迟时间，默认1s，服务端会控制到10s
            long delayTime = DEFAULT_DELAY;

            try {
                ServiceInfo serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));

                if (serviceObj == null) {
                    // 从服务端获取实时注册表，更新至内存
                    updateService(serviceName, clusters);
                    return;
                }

                if (serviceObj.getLastRefTime() <= lastRefTime) {
                    updateService(serviceName, clusters);
                    serviceObj = serviceInfoMap.get(ServiceInfo.getKey(serviceName, clusters));
                } else {
                    // if serviceName already updated by push, we should not override it
                    // since the push data may be different from pull through force push
                    refreshOnly(serviceName, clusters);
                }

                lastRefTime = serviceObj.getLastRefTime();

                if (!notifier.isSubscribed(serviceName, clusters) && !futureMap
                        .containsKey(ServiceInfo.getKey(serviceName, clusters))) {
                    // abort the update task
                    NAMING_LOGGER.info("update task is stopped, service:" + serviceName + ", clusters:" + clusters);
                    return;
                }
                if (CollectionUtils.isEmpty(serviceObj.getHosts())) {
                    incFailCount();
                    return;
                }
                // 使用服务端定义的延迟时间
                delayTime = serviceObj.getCacheMillis();
                resetFailCount();
            } catch (Throwable e) {
                incFailCount();
                NAMING_LOGGER.warn("[NA] failed to update serviceName: " + serviceName, e);
            } finally {
                // 提交延迟任务，执行下一次拉取服务注册表
                executor.schedule(this, Math.min(delayTime << failCount, DEFAULT_DELAY * 60), TimeUnit.MILLISECONDS);
            }
        }
    }
}
