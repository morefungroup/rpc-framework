package com.ouronghui.rpc.registry;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;


/**
 * 服务注册
 */
@Component
public class ServiceRegistry {

    private static final Logger logger = LoggerFactory.getLogger(ServiceRegistry.class);

    @Value("${rpc.registry-address}")
    private String zkAddress;

    private CuratorFramework zkClient;

    @PostConstruct
    public void init() {
        // 创建 Zookeeper 客户端
        zkClient = CuratorFrameworkFactory.newClient(zkAddress, Constant.ZK_SESSION_TIMEOUT, Constant.ZK_CONNECTION_TIMEOUT,
                new RetryNTimes(10, 5000));
        zkClient.start();
        logger.info("connect to zookeeper");
    }

    public void register(String serviceName, String serviceAddress) throws Exception {
        // 创建 registry 节点 (持久)
        String registryPath = Constant.ZK_REGISTRY_PATH;
        Stat stat = zkClient.checkExists().forPath(registryPath);
        if (stat == null) {
            zkClient.create().withMode(CreateMode.PERSISTENT).forPath(registryPath);
            logger.info("create registry node: {}", registryPath);
        }

        // 创建 service 节点(持久)
        String servicePath = registryPath + "/" + serviceName;
        stat = zkClient.checkExists().forPath(servicePath);
        if (stat == null) {
            zkClient.create().withMode(CreateMode.PERSISTENT).forPath(servicePath);
            logger.info("create service node: {}", servicePath);
        }

        // 创建 address 节点（临时,顺序）
        String addressPath = servicePath + "/address-";
        String addressNode = zkClient.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(addressPath);
        zkClient.setData().forPath(addressPath, addressPath.getBytes(StandardCharsets.UTF_8));
        logger.info("create address node: {}", addressNode);
    }
}
