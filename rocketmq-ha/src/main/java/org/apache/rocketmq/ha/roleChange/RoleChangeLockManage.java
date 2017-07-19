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
package org.apache.rocketmq.ha.roleChange;

import java.util.concurrent.ConcurrentHashMap;

public class RoleChangeLockManage {

    public static final long ROLRCHANGE_LOCK_MAX_LIVE_TIME = 1000 * 30;

    private final ConcurrentHashMap<String/* addr */, Long/* time */> roleChangeLockTable = new ConcurrentHashMap<String, Long>(1024);

    public boolean tryLock(final String addr) {
        Long lockTime = roleChangeLockTable.get(addr);
        if (lockTime == null) {
            roleChangeLockTable.put(addr, System.currentTimeMillis());
            return true;
        } else {
            boolean expired = (System.currentTimeMillis() - lockTime) > ROLRCHANGE_LOCK_MAX_LIVE_TIME;
            if (expired) {
                roleChangeLockTable.put(addr, System.currentTimeMillis());
                return true;
            }
        }
        return false;
    }

    public boolean unLock(final String addr) {
        Long flag = roleChangeLockTable.get(addr);
        if (flag != null) {
            roleChangeLockTable.remove(addr);
            return true;
        }
        return false;
    }

}
