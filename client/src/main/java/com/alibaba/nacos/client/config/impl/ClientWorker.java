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

package com.alibaba.nacos.client.config.impl;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.config.ConfigType;
import com.alibaba.nacos.api.config.listener.Listener;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.client.config.common.GroupKey;
import com.alibaba.nacos.client.config.filter.impl.ConfigFilterChainManager;
import com.alibaba.nacos.client.config.http.HttpAgent;
import com.alibaba.nacos.client.config.utils.ContentUtils;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.client.utils.ParamUtil;
import com.alibaba.nacos.client.utils.TenantUtil;
import com.alibaba.nacos.common.http.HttpRestResult;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.MD5Utils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.api.common.Constants.CONFIG_TYPE;
import static com.alibaba.nacos.api.common.Constants.LINE_SEPARATOR;
import static com.alibaba.nacos.api.common.Constants.WORD_SEPARATOR;

/**
 * Long polling.
 *
 * @author Nacos
 */
public class ClientWorker implements Closeable {

    private static final Logger LOGGER = LogUtils.logger(ClientWorker.class);

    /**
     * Add listeners for data.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     */
    public void addListeners(String dataId, String group, List<? extends Listener> listeners) {
        group = null2defaultGroup(group);
        CacheData cache = addCacheDataIfAbsent(dataId, group);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listener.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        CacheData cache = getCache(dataId, group);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group);
            }
        }
    }

    /**
     * Add listeners for tenant.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListeners(String dataId, String group, List<? extends Listener> listeners)
            throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        //获取cacheData
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        //给cacheData注册监听器
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Add listeners for tenant with content.
     *
     * @param dataId    dataId of data
     * @param group     group of data
     * @param content   content
     * @param listeners listeners
     * @throws NacosException nacos exception
     */
    public void addTenantListenersWithContent(String dataId, String group, String content,
            List<? extends Listener> listeners) throws NacosException {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = addCacheDataIfAbsent(dataId, group, tenant);
        cache.setContent(content);
        for (Listener listener : listeners) {
            cache.addListener(listener);
        }
    }

    /**
     * Remove listeners for tenant.
     *
     * @param dataId   dataId of data
     * @param group    group of data
     * @param listener listener
     */
    public void removeTenantListener(String dataId, String group, Listener listener) {
        group = null2defaultGroup(group);
        String tenant = agent.getTenant();
        CacheData cache = getCache(dataId, group, tenant);
        if (null != cache) {
            cache.removeListener(listener);
            if (cache.getListeners().isEmpty()) {
                removeCache(dataId, group, tenant);
            }
        }
    }

    private void removeCache(String dataId, String group) {
        String groupKey = GroupKey.getKey(dataId, group);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", this.agent.getName(), groupKey);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    void removeCache(String dataId, String group, String tenant) {
        String groupKey = GroupKey.getKeyTenant(dataId, group, tenant);
        cacheMap.remove(groupKey);
        LOGGER.info("[{}] [unsubscribe] {}", agent.getName(), groupKey);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group) {
        String key = GroupKey.getKey(dataId, group);
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }

        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group);
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        if (lastCacheData == null) {
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();
            lastCacheData = cacheData;
            lastCacheData.setTaskId(taskId);
        }
        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", this.agent.getName(), key);

        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
        return lastCacheData;
    }

    /**
     * Add cache data if absent.
     *
     * @param dataId data id if data
     * @param group  group of data
     * @param tenant tenant of data
     * @return cache data
     */
    public CacheData addCacheDataIfAbsent(String dataId, String group, String tenant) throws NacosException {
        String key = GroupKey.getKeyTenant(dataId, group, tenant);
        // 1 如果缓存中已经存在，直接返回
        CacheData cacheData = cacheMap.get(key);
        if (cacheData != null) {
            return cacheData;
        }
        // 2 创建CacheData，这里会使用本地配置文件设置为初始配置
        cacheData = new CacheData(configFilterChainManager, agent.getName(), dataId, group, tenant);
        // 3 多线程操作cacheMap再次校验是否已经缓存了cacheData
        // multiple listeners on the same dataid+group and race condition
        CacheData lastCacheData = cacheMap.putIfAbsent(key, cacheData);
        // 4 如果当前线程成功设置了key-cacheData，返回cacheData
        if (lastCacheData == null) {
            //fix issue # 1317
            if (enableRemoteSyncConfig) {// 是否允许添加监听时实时同步配置，默认false
                String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                cacheData.setContent(ct[0]);
            }
            // 计算所属长轮询任务id
            int taskId = cacheMap.size() / (int) ParamUtil.getPerTaskConfigSize();

            cacheData.setTaskId(taskId);
            lastCacheData = cacheData;
        }

        // 这里设置cacheData正在初始化，让下次长轮询立即返回结果
        // reset so that server not hang this check
        lastCacheData.setInitializing(true);

        LOGGER.info("[{}] [subscribe] {}", agent.getName(), key);
        MetricsMonitor.getListenConfigCountMonitor().set(cacheMap.size());
        // 5 否则返回的cacheData是老的cacheData
        return lastCacheData;
    }

    public CacheData getCache(String dataId, String group) {
        return getCache(dataId, group, TenantUtil.getUserTenantForAcm());
    }

    public CacheData getCache(String dataId, String group, String tenant) {
        if (null == dataId || null == group) {
            throw new IllegalArgumentException();
        }
        return cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
    }

    public String[] getServerConfig(String dataId, String group, String tenant, long readTimeout)
            throws NacosException {
        String[] ct = new String[2];
        if (StringUtils.isBlank(group)) {
            group = Constants.DEFAULT_GROUP;
        }

        HttpRestResult<String> result = null;
        try {
            Map<String, String> params = new HashMap<String, String>(3);
            if (StringUtils.isBlank(tenant)) {
                params.put("dataId", dataId);
                params.put("group", group);
            } else {
                params.put("dataId", dataId);
                params.put("group", group);
                params.put("tenant", tenant);
            }
            // 1. 请求/v1/cs/configs
            result = agent.httpGet(Constants.CONFIG_CONTROLLER_PATH, null, params, agent.getEncode(), readTimeout);
        } catch (Exception ex) {
            String message = String
                    .format("[%s] [sub-server] get server config exception, dataId=%s, group=%s, tenant=%s",
                            agent.getName(), dataId, group, tenant);
            LOGGER.error(message, ex);
            throw new NacosException(NacosException.SERVER_ERROR, ex);
        }
        // 2. 处理返回结果，如果200和404，更新本地snapshot文件
        switch (result.getCode()) {
            case HttpURLConnection.HTTP_OK:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, result.getData());
                ct[0] = result.getData();
                if (result.getHeader().getValue(CONFIG_TYPE) != null) {
                    ct[1] = result.getHeader().getValue(CONFIG_TYPE);
                } else {
                    ct[1] = ConfigType.TEXT.getType();
                }
                return ct;
            case HttpURLConnection.HTTP_NOT_FOUND:
                LocalConfigInfoProcessor.saveSnapshot(agent.getName(), dataId, group, tenant, null);
                return ct;
            case HttpURLConnection.HTTP_CONFLICT: {
                LOGGER.error(
                        "[{}] [sub-server-error] get server config being modified concurrently, dataId={}, group={}, "
                                + "tenant={}", agent.getName(), dataId, group, tenant);
                throw new NacosException(NacosException.CONFLICT,
                        "data being modified, dataId=" + dataId + ",group=" + group + ",tenant=" + tenant);
            }
            case HttpURLConnection.HTTP_FORBIDDEN: {
                LOGGER.error("[{}] [sub-server-error] no right, dataId={}, group={}, tenant={}", agent.getName(),
                        dataId, group, tenant);
                throw new NacosException(result.getCode(), result.getMessage());
            }
            default: {
                LOGGER.error("[{}] [sub-server-error]  dataId={}, group={}, tenant={}, code={}", agent.getName(),
                        dataId, group, tenant, result.getCode());
                throw new NacosException(result.getCode(),
                        "http error, code=" + result.getCode() + ",dataId=" + dataId + ",group=" + group + ",tenant="
                                + tenant);
            }
        }
    }

    private void checkLocalConfig(CacheData cacheData) {
        final String dataId = cacheData.dataId;
        final String group = cacheData.group;
        final String tenant = cacheData.tenant;
        File path = LocalConfigInfoProcessor.getFailoverFile(agent.getName(), dataId, group, tenant);
        // 当isUseLocalConfigInfo=false（不使用failover配置文件） 且 failover配置文件存在时，使用failover配置文件，并更新内存中的配置
        if (!cacheData.isUseLocalConfigInfo() && path.exists()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);

            LOGGER.warn(
                    "[{}] [failover-change] failover file created. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
            return;
        }
        // 当isUseLocalConfigInfo=true 或 failover配置文件不存在时，不使用failover配置文件
        // If use local config info, then it doesn't notify business listener and notify after getting from server.
        if (cacheData.isUseLocalConfigInfo() && !path.exists()) {
            cacheData.setUseLocalConfigInfo(false);
            LOGGER.warn("[{}] [failover-change] failover file deleted. dataId={}, group={}, tenant={}", agent.getName(),
                    dataId, group, tenant);
            return;
        }

        // When it changed.
        // 当isUseLocalConfigInfo=true 且 failover配置文件存在时，使用failover配置文件并更新内存中的配置
        if (cacheData.isUseLocalConfigInfo() && path.exists() && cacheData.getLocalConfigInfoVersion() != path
                .lastModified()) {
            String content = LocalConfigInfoProcessor.getFailover(agent.getName(), dataId, group, tenant);
            final String md5 = MD5Utils.md5Hex(content, Constants.ENCODE);
            cacheData.setUseLocalConfigInfo(true);
            cacheData.setLocalConfigInfoVersion(path.lastModified());
            cacheData.setContent(content);
            LOGGER.warn(
                    "[{}] [failover-change] failover file changed. dataId={}, group={}, tenant={}, md5={}, content={}",
                    agent.getName(), dataId, group, tenant, md5, ContentUtils.truncateContent(content));
        }
    }

    private String null2defaultGroup(String group) {
        return (null == group) ? Constants.DEFAULT_GROUP : group.trim();
    }

    /**
     * Check config info.
     */
    public void checkConfigInfo() {
        // Dispatch taskes.
        // cacheMap大小
        int listenerSize = cacheMap.size();
        // Round up the longingTaskCount.
        // cacheMap大小 / 3000 向上取整
        int longingTaskCount = (int) Math.ceil(listenerSize / ParamUtil.getPerTaskConfigSize());
        // 计算longingTaskCount 大于 当前实际长轮询任务数量
        if (longingTaskCount > currentLongingTaskCount) {

            for (int i = (int) currentLongingTaskCount; i < longingTaskCount; i++) {
                // The task list is no order.So it maybe has issues when changing.
                // 开启新的长轮询任务
                /**
                 * 所以开启长轮询任务的时机，一般是注册监听之后创建了CacheData，checkConfigInfo定时任务扫描到需要开启新的长轮询任务时，触发长轮询任务提交
                 */
                executorService.execute(new LongPollingRunnable(i));
            }
            currentLongingTaskCount = longingTaskCount;
        }
    }

    /**
     * Fetch the dataId list from server.
     *
     * @param cacheDatas              CacheDatas for config infomations.
     * @param inInitializingCacheList initial cache lists.
     * @return String include dataId and group (ps: it maybe null).
     * @throws Exception Exception.
     */
    List<String> checkUpdateDataIds(List<CacheData> cacheDatas, List<String> inInitializingCacheList) throws Exception {
        StringBuilder sb = new StringBuilder();
        for (CacheData cacheData : cacheDatas) {
            //不使用failover配置文件
            /**
             * 这里会统计所有非failover配置，并拼接请求业务报文：
             *
             * 有namespace的CacheData：dataId group md5 namespace
             * 无namespace的CacheData：dataId group md5
             */
            if (!cacheData.isUseLocalConfigInfo()) {
                sb.append(cacheData.dataId).append(WORD_SEPARATOR);
                sb.append(cacheData.group).append(WORD_SEPARATOR);
                if (StringUtils.isBlank(cacheData.tenant)) {
                    sb.append(cacheData.getMd5()).append(LINE_SEPARATOR);
                } else {
                    sb.append(cacheData.getMd5()).append(WORD_SEPARATOR);
                    sb.append(cacheData.getTenant()).append(LINE_SEPARATOR);
                }
                //将首次监听的CacheData放入inInitializingCacheList
                //过滤出了正在初始化的CacheData，即CacheData刚构建，内部content仍然是本地snapshot版本
                if (cacheData.isInitializing()) {
                    // It updates when cacheData occours in cacheMap by first time.
                    inInitializingCacheList
                            .add(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant));
                }
            }
        }
        boolean isInitializingCacheList = !inInitializingCacheList.isEmpty();
        // 实际发起请求
        return checkUpdateConfigStr(sb.toString(), isInitializingCacheList);
    }

    /**
     * Fetch the updated dataId list from server.
     *
     * @param probeUpdateString       updated attribute string value.
     * @param isInitializingCacheList initial cache lists.
     * @return The updated dataId list(ps: it maybe null).
     * @throws IOException Exception.
     */
    List<String> checkUpdateConfigStr(String probeUpdateString, boolean isInitializingCacheList) throws Exception {

        Map<String, String> params = new HashMap<String, String>(2);
        //请求参数Listening-Configs是上面拼接的业务报文
        params.put(Constants.PROBE_MODIFY_REQUEST, probeUpdateString);
        Map<String, String> headers = new HashMap<String, String>(2);
        //长轮询超时时间默认30s，放在请求头Long-Pulling-Timeout里
        headers.put("Long-Pulling-Timeout", "" + timeout);

        // told server do not hang me up if new initializing cacheData added in
        // 告诉服务端，本次长轮询包含首次监听的配置项，不要hold住请求，立即返回
        if (isInitializingCacheList) {
            //如果本次长轮询包含首次监听的配置项，在请求头设置Long-Pulling-Timeout-No-Hangup=true，让服务端立即返回本次轮询结果
            headers.put("Long-Pulling-Timeout-No-Hangup", "true");
        }
        // 如果没有需要监听的
        if (StringUtils.isBlank(probeUpdateString)) {
            return Collections.emptyList();
        }

        try {
            // In order to prevent the server from handling the delay of the client's long task,
            // increase the client's read timeout to avoid this problem.
            // readTimeout = 45s
            long readTimeoutMs = timeout + (long) Math.round(timeout >> 1);
            //    发请求 /v1/cs/configs/listener
            //   服务端/v1/cs/configs/listener接口负责处理长轮询请求
            HttpRestResult<String> result = agent
                    .httpPost(Constants.CONFIG_CONTROLLER_PATH + "/listener", headers, params, agent.getEncode(),
                            readTimeoutMs);

            if (result.ok()) {
                setHealthServer(true);
                //parseUpdateDataIdResponse方法会解析服务端返回报文,每行报文代表一个发生配置变化的groupKey。
                return parseUpdateDataIdResponse(result.getData());
            } else {
                setHealthServer(false);
                LOGGER.error("[{}] [check-update] get changed dataId error, code: {}", agent.getName(),
                        result.getCode());
            }
        } catch (Exception e) {
            setHealthServer(false);
            LOGGER.error("[" + agent.getName() + "] [check-update] get changed dataId exception", e);
            throw e;
        }
        return Collections.emptyList();
    }

    /**
     * Get the groupKey list from the http response.
     *
     * @param response Http response.
     * @return GroupKey List, (ps: it maybe null).
     */
    private List<String> parseUpdateDataIdResponse(String response) {
        if (StringUtils.isBlank(response)) {
            return Collections.emptyList();
        }

        try {
            response = URLDecoder.decode(response, "UTF-8");
        } catch (Exception e) {
            LOGGER.error("[" + agent.getName() + "] [polling-resp] decode modifiedDataIdsString error", e);
        }

        List<String> updateList = new LinkedList<String>();
        //每行报文代表一个发生配置变化的groupKey。按行分割
        for (String dataIdAndGroup : response.split(LINE_SEPARATOR)) {
            if (!StringUtils.isBlank(dataIdAndGroup)) {
                // 每行按空格分割，拼接为dataId+group+namespace 或 dataId+group
                String[] keyArr = dataIdAndGroup.split(WORD_SEPARATOR);
                String dataId = keyArr[0];
                String group = keyArr[1];
                if (keyArr.length == 2) {
                    updateList.add(GroupKey.getKey(dataId, group));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}", agent.getName(), dataId,
                            group);
                } else if (keyArr.length == 3) {
                    String tenant = keyArr[2];
                    updateList.add(GroupKey.getKeyTenant(dataId, group, tenant));
                    LOGGER.info("[{}] [polling-resp] config changed. dataId={}, group={}, tenant={}", agent.getName(),
                            dataId, group, tenant);
                } else {
                    LOGGER.error("[{}] [polling-resp] invalid dataIdAndGroup error {}", agent.getName(),
                            dataIdAndGroup);
                }
            }
        }
        return updateList;
    }

    @SuppressWarnings("PMD.ThreadPoolCreationRule")
    public ClientWorker(final HttpAgent agent, final ConfigFilterChainManager configFilterChainManager,
            final Properties properties) {
        this.agent = agent;
        this.configFilterChainManager = configFilterChainManager;

        // Initialize the timeout parameter
        // 初始化一些参数，如：timeout
        init(properties);
        // 单线程执行器
        this.executor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("com.alibaba.nacos.client.Worker." + agent.getName());
                t.setDaemon(true);
                return t;
            }
        });
        // 执行LongPollingRunnable的执行器，固定线程数=核数
        this.executorService = Executors
                .newScheduledThreadPool(Runtime.getRuntime().availableProcessors(), new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        Thread t = new Thread(r);
                        t.setName("com.alibaba.nacos.client.Worker.longPolling." + agent.getName());
                        t.setDaemon(true);
                        return t;
                    }
                });


        this.executor.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                try {
                    /**  10ms执行一次
                     * 负责检测当前情况（cacheMap大小及当前已经提交的长轮询任务数），是否需要提交新的长轮询任务到executorService中，固定线程数=1。
                     */
                    checkConfigInfo();
                } catch (Throwable e) {
                    LOGGER.error("[" + agent.getName() + "] [sub-check] rotate check error", e);
                }
            }
        }, 1L, 10L, TimeUnit.MILLISECONDS);
    }

    private void init(Properties properties) {

        timeout = Math.max(ConvertUtils.toInt(properties.getProperty(PropertyKeyConst.CONFIG_LONG_POLL_TIMEOUT),
                Constants.CONFIG_LONG_POLL_TIMEOUT), Constants.MIN_CONFIG_LONG_POLL_TIMEOUT);

        taskPenaltyTime = ConvertUtils
                .toInt(properties.getProperty(PropertyKeyConst.CONFIG_RETRY_TIME), Constants.CONFIG_RETRY_TIME);

        this.enableRemoteSyncConfig = Boolean
                .parseBoolean(properties.getProperty(PropertyKeyConst.ENABLE_REMOTE_SYNC_CONFIG));
    }

    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, LOGGER);
        ThreadUtils.shutdownThreadPool(executor, LOGGER);
        LOGGER.info("{} do shutdown stop", className);
    }

    class LongPollingRunnable implements Runnable {

        private final int taskId;

        public LongPollingRunnable(int taskId) {
            this.taskId = taskId;
        }

        /**
         * 1、处理failover配置：判断当前CacheData是否使用failover配置（ClientWorker.checkLocalConfig），如果使用failover配置，则校验本地配置文件内容是否发生变化，发生变化则触发监听器（CacheData.checkListenerMd5）。这一步其实和长轮询无关。
         * 2、对于所有非failover配置，执行长轮询，返回发生改变的groupKey（ClientWorker.checkUpdateDataIds）。
         * 3、根据返回的groupKey，查询服务端实时配置并保存snapshot（ClientWorker.getServerConfig）
         * 4、更新内存CacheData的配置content。
         * 5、校验配置是否发生变更，通知监听器（CacheData.checkListenerMd5）。
         * 6、如果正常执行本次长轮询，立即提交长轮询任务，执行下一次长轮询；发生异常，延迟2s提交长轮询任务。
         */
        @Override
        public void run() {
            // 当前长轮询任务负责的CacheData集合
            List<CacheData> cacheDatas = new ArrayList<CacheData>();
            // 正在初始化的CacheData 即刚构建的CacheData，内部的content仍然是snapshot版本
            List<String> inInitializingCacheList = new ArrayList<String>();
            try {
                // 1. 对于failover配置文件的处理
                // check failover config
                for (CacheData cacheData : cacheMap.values()) {
                    //当前长轮询任务负责的CacheData
                    if (cacheData.getTaskId() == taskId) {
                        cacheDatas.add(cacheData);
                        try {
                            // 判断cacheData是否需要使用failover配置，设置isUseLocalConfigInfo
                            // 如果需要则更新内存中的配置
                            checkLocalConfig(cacheData);
                            // 使用failover配置则检测content内容是否发生变化，如果变化则通知监听器
                            if (cacheData.isUseLocalConfigInfo()) {
                                cacheData.checkListenerMd5();
                            }
                        } catch (Exception e) {
                            LOGGER.error("get local config info error", e);
                        }
                    }
                }

                // 2. 对于所有非failover配置，执行长轮询，返回发生改变的groupKey
                // check server config
                List<String> changedGroupKeys = checkUpdateDataIds(cacheDatas, inInitializingCacheList);
                if (!CollectionUtils.isEmpty(changedGroupKeys)) {
                    LOGGER.info("get changedGroupKeys:" + changedGroupKeys);
                }

                //每个元素代表一个发生配置变化的groupKey。
                for (String groupKey : changedGroupKeys) {
                    String[] key = GroupKey.parseKey(groupKey);
                    String dataId = key[0];
                    String group = key[1];
                    String tenant = null;
                    if (key.length == 3) {
                        tenant = key[2];
                    }
                    try {
                        // 3. 对于发生改变的配置，查询实时配置并保存snapshot
                        String[] ct = getServerConfig(dataId, group, tenant, 3000L);
                        // 4. 更新内存中的配置
                        CacheData cache = cacheMap.get(GroupKey.getKeyTenant(dataId, group, tenant));
                        cache.setContent(ct[0]);
                        if (null != ct[1]) {
                            cache.setType(ct[1]);
                        }
                        LOGGER.info("[{}] [data-received] dataId={}, group={}, tenant={}, md5={}, content={}, type={}",
                                agent.getName(), dataId, group, tenant, cache.getMd5(),
                                ContentUtils.truncateContent(ct[0]), ct[1]);
                    } catch (NacosException ioe) {
                        String message = String
                                .format("[%s] [get-update] get changed config exception. dataId=%s, group=%s, tenant=%s",
                                        agent.getName(), dataId, group, tenant);
                        LOGGER.error(message, ioe);
                    }
                }
                // 5. 对于非failover配置，触发监听器
                for (CacheData cacheData : cacheDatas) {
                    // 排除failover文件
                    if (!cacheData.isInitializing() || inInitializingCacheList
                            .contains(GroupKey.getKeyTenant(cacheData.dataId, cacheData.group, cacheData.tenant))) {
                        // 校验md5是否发生变化，如果发生变化通知listener
                        cacheData.checkListenerMd5();
                        cacheData.setInitializing(false);
                    }
                }
                inInitializingCacheList.clear();
                // 6-1. 都执行完成以后，再次提交长轮询任务
                executorService.execute(this);

            } catch (Throwable e) {

                // If the rotation training task is abnormal, the next execution time of the task will be punished
                LOGGER.error("longPolling error : ", e);
                // 6-2. 如果长轮询执行发生异常，延迟2s执行下一次长轮询
                executorService.schedule(this, taskPenaltyTime, TimeUnit.MILLISECONDS);
            }
        }
    }

    public boolean isHealthServer() {
        return isHealthServer;
    }

    private void setHealthServer(boolean isHealthServer) {
        this.isHealthServer = isHealthServer;
    }

    /**
     *
     * 	  负责检测当前情况（cacheMap大小及当前已经提交的长轮询任务数），是否需要提交新的长轮询任务到executorService中，固定线程数=1。
     */
    final ScheduledExecutorService executor;

    /**
     * 执行长轮询，一般情况下执行listener回调也是在这个线程里 固定线程数=核数
     */
    final ScheduledExecutorService executorService;

    /**
     * groupKey -> cacheData.
     * groupKey=dataId+group{+namespace}。
     */
    private final ConcurrentHashMap<String, CacheData> cacheMap = new ConcurrentHashMap<String, CacheData>();

    /**
     * 认为是个httpClient
     */
    private final HttpAgent agent;

    /**
     * 钩子管理器
     */
    private final ConfigFilterChainManager configFilterChainManager;

    /**
     * nacos服务端是否健康
     */
    private boolean isHealthServer = true;

    /**
     * 长轮询超时时间 默认30s
     */
    private long timeout;

    /**
     * 当前长轮询任务数量
     */
    private double currentLongingTaskCount = 0;

    /**
     *  长轮询发生异常，默认延迟2s进行下次长轮询
     */
    private int taskPenaltyTime;

    /**
     * 是否在添加监听器时，主动获取最新配置
     */
    private boolean enableRemoteSyncConfig = false;
}
