package com.chinamobile.iot.thrift.rpc;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.pool.impl.GenericObjectPool;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;

import com.chinamobile.iot.thrift.rpc.ThriftClientPoolFactory.PoolOperationCallBack;
import com.chinamobile.iot.thrift.rpc.zookeeper.ThriftServerAddressProvider;

/**
 * 客户端代理
 */
@SuppressWarnings({ "unchecked", "rawtypes" })
public class ThriftServiceClientProxyFactory
        implements FactoryBean, InitializingBean, Closeable {

    private Logger logger = LoggerFactory.getLogger(getClass());

    private Integer maxActive = 32;// 最大活跃连接数

    private Integer maxIdle = 32;// 最大空闲数量

    private Integer retries = 2;// 最大失败后重试次数

    // ms,default 3 min,链接空闲时间
    // -1,关闭空闲检测
    private Integer idleTime = 180000;
    private ThriftServerAddressProvider serverAddressProvider;

    private Object proxyClient;
    private Class<?> objectClass;
    private TServiceClientFactory<TServiceClient> clientFactory;

    private Map<String, GenericObjectPool<TServiceClientProxy>> poolMap = new Hashtable<>();

    private PoolOperationCallBack callback = new PoolOperationCallBack() {
        @Override
        public void make(TServiceClient client) {
            logger.info("create");
        }

        @Override
        public void destroy(TServiceClient client) {
            logger.info("destroy");
        }
    };

    public void setMaxActive(Integer maxActive) {
        this.maxActive = maxActive;
    }

    public void setIdleTime(Integer idleTime) {
        this.idleTime = idleTime;
    }

    public void setServerAddressProvider(
            ThriftServerAddressProvider serverAddressProvider) {
        this.serverAddressProvider = serverAddressProvider;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        ClassLoader classLoader = Thread.currentThread()
                .getContextClassLoader();
        // 加载Iface接口
        objectClass = classLoader
                .loadClass(serverAddressProvider.getService() + "$Iface");
        // 加载Client.Factory类
        Class<TServiceClientFactory<TServiceClient>> fi = (Class<TServiceClientFactory<TServiceClient>>) classLoader
                .loadClass(
                        serverAddressProvider.getService() + "$Client$Factory");
        clientFactory = fi.newInstance();

        proxyClient = Proxy.newProxyInstance(classLoader,
                new Class[] { objectClass }, new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method,
                            Object[] args) throws Throwable {
                        int count = 0;
                        Object object = null;
                        while (count <= retries) {
                            GenericObjectPool<TServiceClientProxy> pool = borrowTServiceClient();
                            TServiceClientProxy client = pool.borrowObject();
                            boolean flag = true;
                            try {
                                object = method.invoke(
                                        client.gettServiceClient(), args);
                            } catch (Exception e) {
                                flag = false;
                                throw e;
                            } finally {
                                if (flag) {
                                    pool.returnObject(client);
                                    break;
                                } else {
                                    pool.invalidateObject(client);
                                    count++;
                                }
                            }
                        }
                        return object;
                    }
                });
    }

    private GenericObjectPool<TServiceClientProxy> borrowTServiceClient()
            throws Exception {
        InetSocketAddress inetSocketAddress = serverAddressProvider.selector();
        if (poolMap.containsKey(inetSocketAddress.toString())) {
            return poolMap.get(inetSocketAddress.toString());
        }
        return makeTServiceClient(inetSocketAddress);
    }

    private GenericObjectPool<TServiceClientProxy> makeTServiceClient(
            InetSocketAddress inetSocketAddress) throws Exception {
        ThriftClientPoolFactory clientPool = new ThriftClientPoolFactory(
                inetSocketAddress, clientFactory, callback);
        GenericObjectPool.Config poolConfig = new GenericObjectPool.Config();
        poolConfig.maxActive = maxActive;
        poolConfig.maxIdle = maxIdle;
        poolConfig.minEvictableIdleTimeMillis = idleTime;
        poolConfig.timeBetweenEvictionRunsMillis = idleTime * 2L;
        poolConfig.testOnBorrow = true;
        poolConfig.testOnReturn = false;
        poolConfig.testWhileIdle = false;
        // poolConfig.lifo = false; //连接池获取方式false时按照先进先出原则取，默认为true取最近使用的对象
        GenericObjectPool<TServiceClientProxy> pool = new GenericObjectPool<TServiceClientProxy>(
                clientPool, poolConfig);
        poolMap.put(inetSocketAddress.toString(), pool);
        return pool;
    }

    @Override
    public Object getObject() throws Exception {
        return proxyClient;
    }

    @Override
    public Class<?> getObjectType() {
        return objectClass;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void close() {
        Set<Entry<String, GenericObjectPool<TServiceClientProxy>>> entries = poolMap
                .entrySet();
        for (Iterator iterator = entries.iterator(); iterator.hasNext();) {
            Entry<String, GenericObjectPool<TServiceClientProxy>> entry = (Entry<String, GenericObjectPool<TServiceClientProxy>>) iterator
                    .next();
            GenericObjectPool<TServiceClientProxy> pool = entry.getValue();
            if (pool != null) {
                try {
                    pool.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
        if (serverAddressProvider != null) {
            try {
                serverAddressProvider.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
