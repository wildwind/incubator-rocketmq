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

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.rocketmq.common.Configuration;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.ha.config.BrokerHaConfig;
import org.apache.rocketmq.remoting.netty.NettyClientConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerHaController {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final BrokerHaConfig brokerHaConfig;
    // private final NettyServerConfig nettyServerConfig;
    private final NettyClientConfig nettyClientConfig;

    private Configuration configuration;
    private BrokerHaOutService brokerHaOutService;

    private ScheduledExecutorService scheduledExecutorService = Executors
            .newSingleThreadScheduledExecutor(new ThreadFactoryImpl("BrokerHaScheduledThread"));

    public BrokerHaController(BrokerHaConfig brokerHaConfig, NettyClientConfig nettyClientConfig) {
        this.brokerHaConfig = brokerHaConfig;
        this.nettyClientConfig = nettyClientConfig;
        this.brokerHaOutService = new BrokerHaOutService(this, this.nettyClientConfig);

        this.configuration = new Configuration(log, this.brokerHaConfig, this.nettyClientConfig);
    }

    public boolean initialize() {
        if (this.brokerHaConfig.getNamesrvAddr() != null) {
            this.brokerHaOutService.updateNameServerAddressList(this.brokerHaConfig.getNamesrvAddr());
            log.info("Set user specified name server address: {}", this.brokerHaConfig.getNamesrvAddr());
        } else if (this.brokerHaConfig.isFetchNamesrvAddrByAddressServer()) {
            this.scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

                @Override
                public void run() {
                    try {
                        BrokerHaController.this.brokerHaOutService.fetchNameServerAddr();
                    } catch (Throwable e) {
                        log.error("ScheduledTask fetchNameServerAddr exception", e);
                    }
                }
            }, 1000 * 10, 1000 * 60 * 2, TimeUnit.MILLISECONDS);
        }

        scheduledExecutorService.scheduleAtFixedRate(new Runnable() {

            @Override
            public void run() {
                BrokerHaController.this.brokerHaOutService.handleHaChange();
            }
        }, 1000 * 10, brokerHaConfig.getCheckMasterOfflineInterval(), TimeUnit.MILLISECONDS);

        return true;
    }

    public void start() throws Exception {
        if (this.brokerHaOutService != null) {
            this.brokerHaOutService.start();
        }
    }

    public void shutdown() {
        if (this.brokerHaOutService != null) {
            this.brokerHaOutService.shutdown();
        }
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }

    public BrokerHaOutService getBrokerHaOutService() {
        return brokerHaOutService;
    }

    public void setBrokerHaOutService(BrokerHaOutService brokerHaOutService) {
        this.brokerHaOutService = brokerHaOutService;
    }

    public BrokerHaConfig getBrokerHaConfig() {
        return brokerHaConfig;
    }

    public NettyClientConfig getNettyClientConfig() {
        return nettyClientConfig;
    }

}
