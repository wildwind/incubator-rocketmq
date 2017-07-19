package org.apache.rocketmq.ha;

import java.io.UnsupportedEncodingException;
import java.util.Properties;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class BrokerHaOutServiceTest {
    
    private static BrokerHaOutService brokerHaOutService;
    
    public static BrokerHaOutService creatBrokerHaOutService(){
        NettyClientConfig nettyClientConfig=new NettyClientConfig();
        BrokerHaOutService brokerHaOutService=new BrokerHaOutService(null,nettyClientConfig);
        brokerHaOutService.start();
        return brokerHaOutService;
    }
    
    @BeforeClass
    public static void setup(){
        brokerHaOutService=creatBrokerHaOutService();
    }
    @AfterClass
    public static void destory(){
        brokerHaOutService.shutdown();
    }
    @Test
    public void testChangeBrokerConfig() throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException, UnsupportedEncodingException, InterruptedException, MQBrokerException{
        Properties properties=new Properties();
        properties.setProperty("brokerRole", "SLAVE");
        properties.setProperty("brokerId", "1");
        String addr="10.4.120.60:10911";
        brokerHaOutService.changeBrokerConfig(addr, properties);
    }

}
