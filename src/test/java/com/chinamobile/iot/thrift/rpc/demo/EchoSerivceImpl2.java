package com.chinamobile.iot.thrift.rpc.demo;

import org.apache.thrift.TException;

//实现类
public class EchoSerivceImpl2 implements EchoSerivce.Iface {

    @Override
    public String echo(String msg) throws TException {
        return "server2 :" + msg;
    }
}
