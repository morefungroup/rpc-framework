package com.ouronghui.rpc.client;

import com.ouronghui.rpc.registry.ServiceDiscovery;
import com.ouronghui.rpc.common.bean.RpcRequest;
import com.ouronghui.rpc.common.bean.RpcResponse;
import com.ouronghui.rpc.common.codec.RpcDecoder;
import com.ouronghui.rpc.common.codec.RpcEncoder;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RPC 客户端（用于创建 RPC 服务代理）
 */
@Component
public class RpcClient {

    private static final Logger logger = LoggerFactory.getLogger(RpcClient.class);

    @Autowired
    private ServiceDiscovery serviceDisCovery;

    /**
     * 存放请求编号与相应对象之间的映射关系
     */
    private ConcurrentHashMap<String, RpcResponse> responseMap = new ConcurrentHashMap<>();

    public <T> T create(final Class<?> interfaceClass) {
        // 创建动态代理对象
        return (T) Proxy.newProxyInstance(interfaceClass.getClassLoader(), new Class<?>[]{interfaceClass}, (proxy, method, args) -> {
            // 创建 RPC 请求对象
            RpcRequest request = new RpcRequest();
            request.setRequestId(UUID.randomUUID().toString());
            request.setInterfaceName(method.getDeclaringClass().getName());
            request.setMethodName(method.getName());
            request.setParameterTypes(method.getParameterTypes());
            request.setParameters(args);

            // 获取 RPC 服务地址
            String serviceName = interfaceClass.getName();
            String serviceAddress = serviceDisCovery.discover(serviceName);
            logger.info("discover service: {} => {}", serviceName, serviceAddress);

            if (StringUtils.isEmpty(serviceAddress)) {
                throw new RuntimeException("service address is empty");
            }

            // 从 RPC 服务地址中解析主机名与端口号
            String[] array = StringUtils.split(serviceAddress, ":");
            String host = array[0];
            int port = Integer.parseInt(array[1]);

            // 发送 RPC 请求
            RpcResponse response = send(request, host, port);

            if (response == null) {
                logger.error("send request failure", new IllegalStateException("response is null"));
                return null;
            }

            if (response.hasException()) {
                logger.error("response has exception", response.getException());
                return null;
            }

            return response.getResult();
        });
    }

    private RpcResponse send(RpcRequest request, String host, int port) {
        System.out.printf("host: %s, port: %d, interface: %s, method: %s, parameter: %s\n",
                host, port, request.getInterfaceName(), request.getMethodName(), Arrays.toString(request.getParameters()));
        EventLoopGroup group = new NioEventLoopGroup(1); // 单线程模式

        // 创建 RPC 连接
        Bootstrap bootstrap = new Bootstrap();
        bootstrap
                .group(group)
                .channel(NioSocketChannel.class)
                .handler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel socketChannel) throws Exception {
                        socketChannel.pipeline()
                                // 编码 RPC 请求
                                .addLast(new RpcEncoder(RpcRequest.class))
                                // 解码 RPC 响应
                                .addLast(new RpcDecoder(RpcResponse.class))
                                // 处理 RPC 响应
                                .addLast(new RpcClientHandler(responseMap));
                    }
                });

        try {
            ChannelFuture future = bootstrap.connect(host, port).sync();

            // 写入 RPC 请求
            Channel channel = future.channel();
            channel.writeAndFlush(request).sync();
            channel.closeFuture().sync();

            // 获取 RPC 响应
            return responseMap.get(request.getRequestId());
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 关闭 RPC 连接
            group.shutdownGracefully();
            // 移除请求编号与响应对象之间的映射关系
            responseMap.remove(request.getRequestId());
        }

    }
}
