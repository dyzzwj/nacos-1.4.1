package com.dyzwj;

import com.alibaba.nacos.api.NacosFactory;
import com.alibaba.nacos.api.config.ConfigService;
import com.alibaba.nacos.api.config.listener.AbstractListener;
import com.alibaba.nacos.api.exception.NacosException;

import java.util.Properties;
import java.util.Scanner;


/**
 *    Namspace（Tenant）：命名空间（租户），**默认命名空间是public。一个命名空间可以包含多个Group，在Nacos源码里有些变量是tenant租户，和命名空间是一个东西。
 *    Group：组，默认分组是DEFAULT_GROUP。一个组可以包含多个dataId
 *    DataId：译为数据id，在nacos中DataId代表一整个配置文件，是配置的最小单位。和apollo不同，apollo的最小单位是一个配置项key。
 */
public class MyConfigExample {
    public static void main(String[] args) throws NacosException {
        String serverAddr = "localhost";
        String dataId = "cfg0"; // dataId
        String group = "DEFAULT_GROUP"; // group
        Properties properties = new Properties();
        // nacos-server地址
        properties.put("serverAddr", serverAddr);
        // namespace/tenant
        properties.put("namespace", "dca8ec01-bca3-4df9-89ef-8ab299a37f73");
        /**
         * ConfigService是Nacos暴露给客户端的配置服务接口，一个Nacos配置中心+一个Namespace=一个ConfigService实例。
         */
        ConfigService configService = NacosFactory.createConfigService(properties);
        // 1. 注册监听
        configService.addListener(dataId, group, new AbstractListener() {
            @Override
            public void receiveConfigInfo(String configInfo) {
                System.out.println("receive config info ：" + configInfo);
            }
        });
        // 2. 查询初始配置
        String config = configService.getConfig(dataId, group, 3000);
        System.out.println("init config : " + config);
        // 3. 修改配置
        Scanner scanner = new Scanner(System.in);
        while (scanner.hasNext()) {
            String next = scanner.next();
            if ("exit".equals(next)) {
                break;
            }
            configService.publishConfig(dataId, group, next);
        }
    }
}

