/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.watch;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.server.ServerCnxn;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZooTrace;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class manages watches. It allows watches to be associated with a string
 * and removes watchers and their watches in addition to managing triggers.
 */
public class WatchManager implements IWatchManager {

    private static final Logger LOG = LoggerFactory.getLogger(WatchManager.class);

    // 存储path对应的watcher
    private final Map<String, Set<Watcher>> watchTable = new HashMap<>();

    // 存储watcher监听的所有path
    private final Map<Watcher, Set<String>> watch2Paths = new HashMap<>();

    private final WatcherModeManager watcherModeManager = new WatcherModeManager();

    @Override
    public synchronized int size() {
        int result = 0;
        for (Set<Watcher> watches : watchTable.values()) {
            result += watches.size();
        }
        return result;
    }

    private boolean isDeadWatcher(Watcher watcher) {
        return watcher instanceof ServerCnxn && ((ServerCnxn) watcher).isStale();
    }

    @Override
    public boolean addWatch(String path, Watcher watcher) {
        return addWatch(path, watcher, WatcherMode.DEFAULT_WATCHER_MODE);
    }

    @Override
    public synchronized boolean addWatch(String path, Watcher watcher, WatcherMode watcherMode) {
        // 判断watcher是否断开了
        if (isDeadWatcher(watcher)) {
            LOG.debug("Ignoring addWatch with closed cnxn");
            return false;
        }

        // 获取path对应的watcher
        Set<Watcher> list = watchTable.get(path);
        if (list == null) {
            // don't waste memory if there are few watches on a node
            // rehash when the 4th entry is added, doubling size thereafter
            // seems like a good compromise
            list = new HashSet<>(4);
            watchTable.put(path, list);
        }
        list.add(watcher);

        // 获取watcher监听的所有path
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null) {
            // cnxns typically have many watches, so use default cap here
            paths = new HashSet<>();
            watch2Paths.put(watcher, paths);
        }

        // 添加watcher的订阅类型
        // WatcherMode
        // STANDARD(false, false),
        // PERSISTENT(true, false),
        // PERSISTENT_RECURSIVE(true, true)
        //watch 有三种类型，一种是PERSISTENT、一种是PERSISTENT_RECURSIVE、STANDARD，前者是 持久化订阅，
        // 后者是持久化递归订阅，所谓递归订阅就是针对监听的节点的子节点的变化都会触发监听,
         watcherModeManager.setWatcherMode(watcher, path, watcherMode);
        return paths.add(path);
    }

    @Override
    public synchronized void removeWatcher(Watcher watcher) {
        Set<String> paths = watch2Paths.remove(watcher);
        if (paths == null) {
            return;
        }
        for (String p : paths) {
            Set<Watcher> list = watchTable.get(p);
            if (list != null) {
                list.remove(watcher);
                if (list.isEmpty()) {
                    watchTable.remove(p);
                }
            }
            watcherModeManager.removeWatcher(watcher, p);
        }
    }

    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type) {
        return triggerWatch(path, type, null);
    }

    /**
     * 触发监听的方法。客户端的回调
     * @param path znode path
     * @param type the watch event type
     * @param supress the suppressed watcher set
     *
     * @return
     */
    @Override
    public WatcherOrBitSet triggerWatch(String path, EventType type, WatcherOrBitSet supress) {
        //根据类型、连接状态、路径，构建WatchedEvent？
        WatchedEvent e = new WatchedEvent(type, KeeperState.SyncConnected, path);
        // 存储需要回调的watcher
        Set<Watcher> watchers = new HashSet<>();
        //是否是递归watcher，也就是如果针对/mic加了递归的watch，那么如果/mic下有子节点，则 会递归/mic下所有子节点，触发事件通知？
        PathParentIterator pathParentIterator = getPathParentIterator(path);
        synchronized (this) {
            for (String localPath : pathParentIterator.asIterable()) {
                // 获取所有的客户端watcher，也就是客户端连接
                Set<Watcher> thisWatchers = watchTable.get(localPath);
                if (thisWatchers == null || thisWatchers.isEmpty()) {
                    continue;
                }
                // 对于一个path，可能有多个客户端监听，这里需要遍历所有的watcher
                Iterator<Watcher> iterator = thisWatchers.iterator();
                while (iterator.hasNext()) {
                    Watcher watcher = iterator.next();
                    // 获取订阅类型
                    WatcherMode watcherMode = watcherModeManager.getWatcherMode(watcher, localPath);
                    if (watcherMode.isRecursive()) {
                        // 如果是递归订阅，子节点变化也要回调
                        if (type != EventType.NodeChildrenChanged) {
                            watchers.add(watcher);
                        }
                    } else if (!pathParentIterator.atParentPath()) {
                        watchers.add(watcher);
                        // 如果不是持久订阅
                        if (!watcherMode.isPersistent()) {
                            // 删除watcher
                            // 这里直接删除，所以是一次性的订阅
                            iterator.remove();
                            // 删除watcher对应的path
                            Set<String> paths = watch2Paths.get(watcher);
                            if (paths != null) {
                                paths.remove(localPath);
                            }
                        }
                    }
                }
                if (thisWatchers.isEmpty()) {
                    watchTable.remove(localPath);
                }
            }
        }
        // 需要回调的watcher为空，直接返回
        if (watchers.isEmpty()) {
            if (LOG.isTraceEnabled()) {
                ZooTrace.logTraceMessage(LOG, ZooTrace.EVENT_DELIVERY_TRACE_MASK, "No watchers for " + path);
            }
            return null;
        }

        // 遍历watcher，循环回调
        for (Watcher w : watchers) {
            if (supress != null && supress.contains(w)) {
                continue;
            }
            //还记得我们在服务端绑定事件的时候，watcher绑定是是什么？是ServerCnxn， 所以w.process(e)，其
            //实调用的应该是ServerCnxn的process方法。而servercnxn又是一个抽象方法，有两个实现类，分别
            //是：NIOServerCnxn和NettyServerCnxn。那接下来我们扒开NIOServerCnxn这个类的process方法看
            //看究竟是怎么回调的。
            // 回调watcher
            // 这里的w就是NIOServerCnxn
            w.process(e);
        }

        switch (type) {
            case NodeCreated:
                ServerMetrics.getMetrics().NODE_CREATED_WATCHER.add(watchers.size());
                break;

            case NodeDeleted:
                ServerMetrics.getMetrics().NODE_DELETED_WATCHER.add(watchers.size());
                break;

            case NodeDataChanged:
                ServerMetrics.getMetrics().NODE_CHANGED_WATCHER.add(watchers.size());
                break;

            case NodeChildrenChanged:
                ServerMetrics.getMetrics().NODE_CHILDREN_WATCHER.add(watchers.size());
                break;
            default:
                // Other types not logged.
                break;
        }

        return new WatcherOrBitSet(watchers);
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append(watch2Paths.size()).append(" connections watching ").append(watchTable.size()).append(" paths\n");

        int total = 0;
        for (Set<String> paths : watch2Paths.values()) {
            total += paths.size();
        }
        sb.append("Total watches:").append(total);

        return sb.toString();
    }

    @Override
    public synchronized void dumpWatches(PrintWriter pwriter, boolean byPath) {
        if (byPath) {
            for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {
                pwriter.println(e.getKey());
                for (Watcher w : e.getValue()) {
                    pwriter.print("\t0x");
                    pwriter.print(Long.toHexString(((ServerCnxn) w).getSessionId()));
                    pwriter.print("\n");
                }
            }
        } else {
            for (Entry<Watcher, Set<String>> e : watch2Paths.entrySet()) {
                pwriter.print("0x");
                pwriter.println(Long.toHexString(((ServerCnxn) e.getKey()).getSessionId()));
                for (String path : e.getValue()) {
                    pwriter.print("\t");
                    pwriter.println(path);
                }
            }
        }
    }

    @Override
    public synchronized boolean containsWatcher(String path, Watcher watcher) {
        WatcherMode watcherMode = watcherModeManager.getWatcherMode(watcher, path);
        PathParentIterator pathParentIterator = getPathParentIterator(path);
        for (String localPath : pathParentIterator.asIterable()) {
            Set<Watcher> watchers = watchTable.get(localPath);
            if (!pathParentIterator.atParentPath()) {
                if (watchers != null) {
                    return true;    // at the leaf node, all watcher types match
                }
            }
            if (watcherMode.isRecursive()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public synchronized boolean removeWatcher(String path, Watcher watcher) {
        Set<String> paths = watch2Paths.get(watcher);
        if (paths == null || !paths.remove(path)) {
            return false;
        }

        Set<Watcher> list = watchTable.get(path);
        if (list == null || !list.remove(watcher)) {
            return false;
        }

        if (list.isEmpty()) {
            watchTable.remove(path);
        }

        watcherModeManager.removeWatcher(watcher, path);

        return true;
    }

    @Override
    public synchronized WatchesReport getWatches() {
        Map<Long, Set<String>> id2paths = new HashMap<>();
        for (Entry<Watcher, Set<String>> e : watch2Paths.entrySet()) {
            Long id = ((ServerCnxn) e.getKey()).getSessionId();
            Set<String> paths = new HashSet<>(e.getValue());
            id2paths.put(id, paths);
        }
        return new WatchesReport(id2paths);
    }

    @Override
    public synchronized WatchesPathReport getWatchesByPath() {
        Map<String, Set<Long>> path2ids = new HashMap<>();
        for (Entry<String, Set<Watcher>> e : watchTable.entrySet()) {
            Set<Long> ids = new HashSet<>(e.getValue().size());
            path2ids.put(e.getKey(), ids);
            for (Watcher watcher : e.getValue()) {
                ids.add(((ServerCnxn) watcher).getSessionId());
            }
        }
        return new WatchesPathReport(path2ids);
    }

    @Override
    public synchronized WatchesSummary getWatchesSummary() {
        int totalWatches = 0;
        for (Set<String> paths : watch2Paths.values()) {
            totalWatches += paths.size();
        }
        return new WatchesSummary(watch2Paths.size(), watchTable.size(), totalWatches);
    }

    @Override
    public void shutdown() { /* do nothing */ }

    @Override
    public int getRecursiveWatchQty() {
        return watcherModeManager.getRecursiveQty();
    }

    private PathParentIterator getPathParentIterator(String path) {
        if (watcherModeManager.getRecursiveQty() == 0) {
            return PathParentIterator.forPathOnly(path);
        }
        return PathParentIterator.forAll(path);
    }
}
