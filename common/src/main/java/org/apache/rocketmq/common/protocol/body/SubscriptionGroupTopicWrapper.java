package org.apache.rocketmq.common.protocol.body;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.rocketmq.common.DataVersion;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

public class SubscriptionGroupTopicWrapper extends RemotingSerializable {
    private  ConcurrentHashMap<String/*group*/, Set<String>/*topic*/> subscriptionGroupTopicTable = new ConcurrentHashMap<String, Set<String>>(1024);
    private  DataVersion dataVersion = new DataVersion();
    
    public ConcurrentHashMap<String, Set<String>> getSubscriptionGroupTopicTable() {
        return subscriptionGroupTopicTable;
    }
    public void setSubscriptionGroupTopicTable(ConcurrentHashMap<String, Set<String>> subscriptionGroupTopicTable) {
        this.subscriptionGroupTopicTable = subscriptionGroupTopicTable;
    }
    public DataVersion getDataVersion() {
        return dataVersion;
    }
    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }
    
    
}
