package com.morefu.rpc.registry;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 服务发现
 */
@Component
public class ServiceDiscovery {
    private static final Logger logger = LoggerFactory.getLogger(ServiceDiscovery.class);

    @Value("${rpc.registry-address}")
    private String zkAddress;

    public String discover(String name) {
        // 创建 zookeeper 客户端
        CuratorFramework client = CuratorFrameworkFactory.newClient(zkAddress, Constant.ZK_SESSION_TIMEOUT, Constant.ZK_CONNECTION_TIMEOUT,
                new RetryNTimes(10, 5000));
        client.start();
        logger.info("connected to zookeeper");
        try {
            // 获取 service 节点
            String servicePath = Constant.ZK_REGISTRY_PATH + "/" + name;

            Stat stat = client.checkExists().forPath(servicePath);
            // TODO no service node
            if (stat == null) {
                throw new RuntimeException(String.format("can not find any service node on path: %s", servicePath));
            }

            List<String> addressList = client.getChildren().forPath(servicePath);

            // TODO no address
            if (CollectionUtils.isEmpty(addressList)) {
                throw new RuntimeException(String.format("can not find any address node on path: %s", servicePath));
            }

            // 获取 address 节点
            String address;
            int size = addressList.size();
            if (size == 1) {
                // 若地址只有一个,则获取该地址
                address = addressList.get(0);
                logger.info("get only address node: {}", address);
            } else {
                // 若存在多个地址, 则随机获取一个地址
                // loadbalancer
                // metadata
                address = addressList.get(ThreadLocalRandom.current().nextInt(size));
                logger.info("get random address node: {}", address);
            }
            // 获取 address 节点的值
            String addressPath = servicePath + "/" + address;
            return new String(client.getData().forPath(addressPath));
        } catch (Exception e) {
            logger.error("service discovery exception:", e);
            throw new RuntimeException(e);
        } finally {
            client.close();
        }
    }
}
