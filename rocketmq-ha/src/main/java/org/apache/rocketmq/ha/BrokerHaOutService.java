/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.ha;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.namesrv.TopAddressing;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.ha.roleChange.RoleChangeLockManage;
import org.apache.rocketmq.remoting.RemotingClient;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.apache.rocketmq.remoting.netty.NettyRemotingClient;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerHaOutService {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_CONSOLE_NAME);

    private static final long TIMEOUTMILLIS = 3000;

    private final BrokerHaController brokerHaController;
    private final RemotingClient remotingClient;
    private final TopAddressing topAddressing = new TopAddressing(MixAll.getWSAddr());

    private RoleChangeLockManage roleChangeLock = new RoleChangeLockManage();
    private String nameSrvAddr = null;

    public BrokerHaOutService(BrokerHaController brokerHaController, final NettyClientConfig nettyClientConfig) {
        this.brokerHaController = brokerHaController;
        this.remotingClient = new NettyRemotingClient(nettyClientConfig);
    }

    public String fetchNameServerAddr() {
        try {
            String addrs = this.topAddressing.fetchNSAddr();
            if (addrs != null) {
                if (!addrs.equals(this.nameSrvAddr)) {
                    log.info("name server address changed, old: {} new: {}", this.nameSrvAddr, addrs);
                    this.updateNameServerAddressList(addrs);
                    this.nameSrvAddr = addrs;
                    return nameSrvAddr;
                }
            }
        } catch (Exception e) {
            log.error("fetchNameServerAddr Exception", e);
        }
        return nameSrvAddr;
    }

    public void updateNameServerAddressList(final String addrs) {
        List<String> lst = new ArrayList<String>();
        String[] addrArray = addrs.split(";");
        for (String addr : addrArray) {
            lst.add(addr);
        }

        this.remotingClient.updateNameServerAddressList(lst);
    }

    public void start() {
        this.remotingClient.start();
    }

    public void shutdown() {
        this.remotingClient.shutdown();
    }

    public void handleHaChange() {
        try {
            ClusterInfo clusterInfo = getBrokerClusterInfo();
            log.info("cluster info {}", clusterInfo.toJson());
            List<String> needChangeSlavers = fickupNeedChangeSlaverBroker(clusterInfo);
            log.info("find needChange brokers {}", needChangeSlavers);
            changeAllBrokerRole(needChangeSlavers);
        } catch (RemotingTimeoutException | RemotingSendRequestException | RemotingConnectException | InterruptedException | MQBrokerException e) {
            e.printStackTrace();
        }
    }

    public ClusterInfo getBrokerClusterInfo()
            throws InterruptedException, RemotingTimeoutException, RemotingSendRequestException, RemotingConnectException, MQBrokerException {
        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.GET_BROKER_CLUSTER_INFO, null);

        RemotingCommand response = this.remotingClient.invokeSync(null, request, TIMEOUTMILLIS);
        assert response != null;
        switch (response.getCode()) {
            case ResponseCode.SUCCESS: {
                return ClusterInfo.decode(response.getBody(), ClusterInfo.class);
            }
            default:
                break;
        }
        return null;
    }

    private List<String> fickupNeedChangeSlaverBroker(ClusterInfo clusterInfo) {
        List<String> offlineMasterBrokers = new LinkedList<>();
        HashMap<String, BrokerData> onlineBrokers = clusterInfo.getBrokerAddrTable();
        Iterator<Entry<String, BrokerData>> it = onlineBrokers.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerData> entry = it.next();
            BrokerData brokerData = entry.getValue();
            if (brokerData != null) {
                Map<Long, String> brokerAddrs = brokerData.getBrokerAddrs();
                String addr = brokerAddrs.get(MixAll.MASTER_ID);
                if (addr == null && brokerAddrs.size() > 0) {
                    Set<Long> value = brokerAddrs.keySet();
                    List<Long> brokerIds = new ArrayList<Long>(value);
                    Collections.sort(brokerIds);
                    offlineMasterBrokers.add(brokerAddrs.get(brokerIds.get(0)));
                }
            }
        }
        return offlineMasterBrokers;
    }

    public void changeBrokerRole(final String addr) throws RemotingConnectException, RemotingSendRequestException, RemotingTimeoutException,
            InterruptedException, MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        Properties properties = new Properties();
        properties.setProperty("brokerRole", "ASYNC_MASTER");
        properties.setProperty("brokerId", "0");
        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(false, addr), request, TIMEOUTMILLIS);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public void changeBrokerConfig(final String addr, Properties properties) throws RemotingConnectException, RemotingSendRequestException,
            RemotingTimeoutException, InterruptedException, MQBrokerException, UnsupportedEncodingException {

        RemotingCommand request = RemotingCommand.createRequestCommand(RequestCode.UPDATE_BROKER_CONFIG, null);
        String str = MixAll.properties2String(properties);
        if (str != null && str.length() > 0) {
            request.setBody(str.getBytes(MixAll.DEFAULT_CHARSET));
            RemotingCommand response = this.remotingClient.invokeSync(MixAll.brokerVIPChannel(false, addr), request, TIMEOUTMILLIS);
            switch (response.getCode()) {
                case ResponseCode.SUCCESS: {
                    return;
                }
                default:
                    break;
            }

            throw new MQBrokerException(response.getCode(), response.getRemark());
        }
    }

    public void changeAllBrokerRole(List<String> addrs) {
        for (String addr : addrs) {
            try {
                if (roleChangeLock.tryLock(addr)) {
                    changeBrokerRole(addr);
                    log.info("send change role request to {}", addr);
                } else {
                    log.info("brokerHa is changing the role of {}", addr);
                }

            } catch (RemotingConnectException | RemotingSendRequestException | RemotingTimeoutException | UnsupportedEncodingException
                    | InterruptedException | MQBrokerException e) {
                log.error("change broker role ip:{} error!" + addr);
            }
        }
    }

}
