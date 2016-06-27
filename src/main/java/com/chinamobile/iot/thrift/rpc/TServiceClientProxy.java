package com.chinamobile.iot.thrift.rpc;

import java.net.InetSocketAddress;

import org.apache.thrift.TServiceClient;

public class TServiceClientProxy{
    private TServiceClient tServiceClient;
    private InetSocketAddress inetSocketAddress;
    
    public TServiceClientProxy(TServiceClient tServiceClient) {
        super();
        this.tServiceClient = tServiceClient;
    }
    
    public TServiceClientProxy(TServiceClient tServiceClient,InetSocketAddress inetSocketAddress) {
        super();
        this.tServiceClient = tServiceClient;
        this.inetSocketAddress = inetSocketAddress;
    }


    public TServiceClient gettServiceClient() {
        return tServiceClient;
    }

    public void settServiceClient(TServiceClient tServiceClient) {
        this.tServiceClient = tServiceClient;
    }

    public InetSocketAddress getInetSocketAddress() {
        return inetSocketAddress;
    }

    public void setInetSocketAddress(InetSocketAddress inetSocketAddress) {
        this.inetSocketAddress = inetSocketAddress;
    }
}
