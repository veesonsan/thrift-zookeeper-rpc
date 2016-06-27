package com.chinamobile.iot.thrift.rpc;

import java.net.InetSocketAddress;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.thrift.TServiceClient;
import org.apache.thrift.TServiceClientFactory;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 连接池,thrift-client for spring
 */
public class ThriftClientPoolFactory
        extends BasePoolableObjectFactory<TServiceClientProxy> {

    private Logger logger = LoggerFactory.getLogger(getClass());

    // private final ThriftServerAddressProvider serverAddressProvider;
    private final AtomicReference<InetSocketAddress> inetSocketAddress = new AtomicReference<InetSocketAddress>();
    // InetSocketAddress inetSocketAddress;
    private final TServiceClientFactory<TServiceClient> clientFactory;
    private PoolOperationCallBack callback;

    protected ThriftClientPoolFactory(InetSocketAddress inetSocketAddress,
            TServiceClientFactory<TServiceClient> clientFactory)
            throws Exception {
        this.inetSocketAddress.set(inetSocketAddress);
        this.clientFactory = clientFactory;
    }

    protected ThriftClientPoolFactory(InetSocketAddress inetSocketAddress,
            TServiceClientFactory<TServiceClient> clientFactory,
            PoolOperationCallBack callback) throws Exception {
        this.inetSocketAddress.set(inetSocketAddress);
        this.clientFactory = clientFactory;
        this.callback = callback;
    }

    static interface PoolOperationCallBack {
        // 销毁client之前执行
        void destroy(TServiceClient client);

        // 创建成功是执行
        void make(TServiceClient client);
    }

    @Override
    public void destroyObject(TServiceClientProxy client) throws Exception {
        if (callback != null) {
            try {
                callback.destroy(client.gettServiceClient());
                logger.info("destroyObject:{}", client);
                TTransport pin = client.gettServiceClient().getInputProtocol()
                        .getTransport();
                pin.close();
                TTransport pout = client.gettServiceClient().getOutputProtocol()
                        .getTransport();
                pout.close();
            } catch (Exception e) {
                logger.warn("destroyObject:{}", e);
            }
        }
    }

    @Override
    public void activateObject(TServiceClientProxy client) throws Exception {
    }

    @Override
    public void passivateObject(TServiceClientProxy client) throws Exception {
    }

    @Override
    public boolean validateObject(TServiceClientProxy client) {
        try {
            TTransport pin = client.gettServiceClient().getInputProtocol()
                    .getTransport();
            TTransport pout = client.gettServiceClient().getOutputProtocol()
                    .getTransport();
            InetSocketAddress address = inetSocketAddress.get();
            return pin.isOpen() && pout.isOpen()
                    && client.getInetSocketAddress().getHostName()
                            .equals(address.getHostName())
                    && client.getInetSocketAddress().getPort() == address
                            .getPort();
        } catch (Exception e) {
            logger.warn("validateObject:{}", e);
            return false;
        }
    }

    @Override
    public TServiceClientProxy makeObject() throws Exception {
        InetSocketAddress address = inetSocketAddress.get();
        TSocket tsocket = new TSocket(address.getHostName(), address.getPort());
        TTransport transport = new TFramedTransport(tsocket);
        TProtocol protocol = new TCompactProtocol(transport);// TBinaryProtocol
        TServiceClient client = this.clientFactory.getClient(protocol);
        transport.open();
        if (callback != null) {
            try {
                callback.make(client);
            } catch (Exception e) {
                logger.warn("makeObject:{}", e);
            }
        }
        return new TServiceClientProxy(client, address);
    }

}
