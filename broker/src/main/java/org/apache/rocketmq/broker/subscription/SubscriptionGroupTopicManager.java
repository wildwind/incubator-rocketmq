package org.apache.rocketmq.broker.subscription;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.BrokerPathConfigHelper;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionGroupTopicManager extends ConfigManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    private final ConcurrentHashMap<String/*group*/, Set<String>/*topic*/> subscriptionGroupTopicTable = new ConcurrentHashMap<String, Set<String>>(1024);
   

    private final DataVersion dataVersion = new DataVersion();
    private transient BrokerController brokerController;
    
    public SubscriptionGroupTopicManager() {
        
    }
    
    public SubscriptionGroupTopicManager(BrokerController brokerController) {
        this.brokerController=brokerController;
    }

    public void updateSubscriptionCroupTopic(String groupName,Set<String> topics){
        Set<String> old = this.subscriptionGroupTopicTable.put(groupName, topics);
        if (old != null) {
            log.info("update subscription group topic relation info config, group: {} old: {} new: {}",groupName, old, topics);
        } else {
            log.info("create new subscription group topic relation info config, group: {} topics:{}", groupName,topics);
        }
        this.dataVersion.nextVersion();

        this.persist();
    }
    
    public Set<String> selectSubscriptionGroupTopics(String groupName){
        return this.subscriptionGroupTopicTable.get(groupName);
    }
    @Override
    public String encode() {
        return this.encode(false);
    }

    @Override
    public String configFilePath() {
        return BrokerPathConfigHelper.getSubscriptionGroupTopicPath(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
    }

    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            SubscriptionGroupTopicManager obj = RemotingSerializable.fromJson(jsonString, SubscriptionGroupTopicManager.class);
            if (obj != null) {
                this.subscriptionGroupTopicTable.putAll(obj.subscriptionGroupTopicTable);
                this.dataVersion.assignNewOne(obj.dataVersion);
                this.printLoadDataWhenFirstBoot(obj);
            }
        }

    }

    @Override
    public String encode(boolean prettyFormat) {
        return RemotingSerializable.toJson(this, prettyFormat);
    }
    
    private void printLoadDataWhenFirstBoot(final SubscriptionGroupTopicManager sgtm) {
        Iterator<Entry<String, Set<String>>> it = sgtm.getSubscriptionGroupTopicTable().entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, Set<String>> next = it.next();
            log.info("load exist subscription group and topic relation info, {}=>{}", next.getKey(),next.getValue().toString());
        }
    }
    
    public ConcurrentHashMap<String, Set<String>> getSubscriptionGroupTopicTable() {
        return subscriptionGroupTopicTable;
    }
    
    public DataVersion getDataVersion() {
        return dataVersion;
    }

}
