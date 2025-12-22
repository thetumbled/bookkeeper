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

package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.COOKIE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BKException.ZKException;
import org.apache.bookkeeper.common.concurrent.FutureUtils;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats.BookieServiceInfoFormat;
import org.apache.bookkeeper.versioning.LongVersion;
import org.apache.bookkeeper.versioning.Version;
import org.apache.bookkeeper.versioning.Version.Occurred;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.zookeeper.AddWatchMode;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

/**
 * ZooKeeper based {@link RegistrationClient}.
 */

@Slf4j
public class ZKRegistrationClient implements RegistrationClient {

    static final int ZK_CONNECT_BACKOFF_MS = 200;

    class WatchTask
        implements Runnable,
                   Watcher,
                   BiConsumer<Versioned<Set<BookieId>>, Throwable>,
                   AutoCloseable {

        private final String regPath;
        private final Set<RegistrationListener> listeners;
        private volatile boolean closed = false;
        private Set<BookieId> bookies = null;
        private Version version = Version.NEW;
        private final CompletableFuture<Void> firstRunFuture;

        WatchTask(String regPath, CompletableFuture<Void> firstRunFuture) {
            this.regPath = regPath;
            this.listeners = new CopyOnWriteArraySet<>();
            this.firstRunFuture = firstRunFuture;
        }

        public int getNumListeners() {
            return listeners.size();
        }

        public boolean addListener(RegistrationListener listener) {
            if (listeners.add(listener)) {
                if (null != bookies) {
                    scheduler.execute(() -> {
                            listener.onBookiesChanged(
                                    new Versioned<>(bookies, version));
                        });
                }
            }
            return true;
        }

        public boolean removeListener(RegistrationListener listener) {
            return listeners.remove(listener);
        }

        void watch() {
            scheduleWatchTask(0L);
        }

        private void scheduleWatchTask(long delayMs) {
            try {
                scheduler.schedule(this, delayMs, TimeUnit.MILLISECONDS);
            } catch (RejectedExecutionException ree) {
                log.warn("Failed to schedule watch bookies task", ree);
            }
        }

        @Override
        public void run() {
            if (isClosed()) {
                return;
            }

            getChildren(regPath, this)
                .whenCompleteAsync(this, scheduler);
        }

        @Override
        public void accept(Versioned<Set<BookieId>> bookieSet, Throwable throwable) {
            if (throwable != null) {
                if (firstRunFuture.isDone()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                } else {
                    firstRunFuture.completeExceptionally(throwable);
                }
                return;
            }

            if (this.version.compare(bookieSet.getVersion()) == Occurred.BEFORE) {
                this.version = bookieSet.getVersion();
                this.bookies = bookieSet.getValue();
                if (!listeners.isEmpty()) {
                    for (RegistrationListener listener : listeners) {
                        listener.onBookiesChanged(bookieSet);
                    }
                }
            }
            FutureUtils.complete(firstRunFuture, null);
        }

        @Override
        public void process(WatchedEvent event) {
            if (EventType.None == event.getType()) {
                if (KeeperState.Expired == event.getState()) {
                    scheduleWatchTask(ZK_CONNECT_BACKOFF_MS);
                }
                return;
            }

            // re-read the bookie list
            scheduleWatchTask(0L);
        }

        boolean isClosed() {
            return closed;
        }

        @Override
        public void close() {
            closed = true;
        }
    }

    private final ZooKeeper zk;
    private final ScheduledExecutorService scheduler;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchWritableBookiesTask = null;
    @Getter(AccessLevel.PACKAGE)
    private WatchTask watchReadOnlyBookiesTask = null;
    private final ConcurrentHashMap<BookieId, Versioned<BookieServiceInfo>> bookieServiceInfoCache =
                                                                            new ConcurrentHashMap<>();
    private final boolean bookieAddressTracking;
    // registration paths
    private final String bookieRegistrationPath;
    private final String bookieAllRegistrationPath;
    private final String bookieReadonlyRegistrationPath;
    // Persistent recursive watch for bookie nodes
    private volatile int persistentWatchCount = 0; // Track how many watches are established (0, 1, or 2)
    private final PersistentRecursiveWatcher persistentRecursiveWatcher;

    public ZKRegistrationClient(ZooKeeper zk,
                                String ledgersRootPath,
                                ScheduledExecutorService scheduler,
                                boolean bookieAddressTracking) {
        this.zk = zk;
        this.scheduler = scheduler;
        // Following Bookie Network Address Changes is an expensive operation
        // as it requires additional ZooKeeper watches
        // we can disable this feature, in case the BK cluster has only
        // static addresses
        this.bookieAddressTracking = bookieAddressTracking;
        this.bookieRegistrationPath = ledgersRootPath + "/" + AVAILABLE_NODE;
        this.bookieAllRegistrationPath = ledgersRootPath + "/" + COOKIE_NODE;
        this.bookieReadonlyRegistrationPath = this.bookieRegistrationPath + "/" + READONLY;
        
        if (bookieAddressTracking) {
            this.persistentRecursiveWatcher = new PersistentRecursiveWatcher();
        } else {
            this.persistentRecursiveWatcher = null;
        }
    }

    @Override
    public void close() {
        // no-op
    }

    public boolean isBookieAddressTracking() {
        return bookieAddressTracking;
    }

    public ZooKeeper getZk() {
        return zk;
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getWritableBookies() {
        return getChildren(bookieRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getAllBookies() {
        return getChildren(bookieAllRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<Set<BookieId>>> getReadOnlyBookies() {
        return getChildren(bookieReadonlyRegistrationPath, null);
    }

    @Override
    public CompletableFuture<Versioned<BookieServiceInfo>> getBookieServiceInfo(BookieId bookieId) {
        // we can only serve data from cache here,
        // because it can happen than this method is called inside the main
        // zookeeper client event loop thread
        Versioned<BookieServiceInfo> resultFromCache = bookieServiceInfoCache.get(bookieId);
        if (log.isDebugEnabled()) {
            log.debug("getBookieServiceInfo {} -> {}", bookieId, resultFromCache);
        }
        if (resultFromCache != null) {
            return CompletableFuture.completedFuture(resultFromCache);
        } else {
            return FutureUtils.exception(new BKException.BKBookieHandleNotAvailableException());
        }
    }


    /**
     * Read BookieServiceInfo from ZooKeeper and updates the local cache.
     *
     * @param bookieId
     * @return an handle to the result of the operation.
     */
    private CompletableFuture<Versioned<BookieServiceInfo>> readBookieServiceInfoAsync(BookieId bookieId) {
        String pathAsWritable = bookieRegistrationPath + "/" + bookieId;
        String pathAsReadonly = bookieReadonlyRegistrationPath + "/" + bookieId;

        CompletableFuture<Versioned<BookieServiceInfo>> promise = new CompletableFuture<>();
        // No watcher needed - using persistent recursive watch on parent node
        zk.getData(pathAsWritable, null,
                (int rc, String path, Object o, byte[] bytes, Stat stat) -> {
            if (KeeperException.Code.OK.intValue() == rc) {
                try {
                    BookieServiceInfo bookieServiceInfo = deserializeBookieServiceInfo(bookieId, bytes);
                    Versioned<BookieServiceInfo> result = new Versioned<>(bookieServiceInfo,
                            new LongVersion(stat.getCversion()));
                    log.info("Update BookieInfoCache (writable bookie) {} -> {}", bookieId, result.getValue());
                    bookieServiceInfoCache.put(bookieId, result);
                    promise.complete(result);
                } catch (IOException ex) {
                    log.error("Cannot update BookieInfo for ", ex);
                    promise.completeExceptionally(KeeperException.create(
                            KeeperException.Code.DATAINCONSISTENCY, path).initCause(ex));
                    return;
                }
            } else if (KeeperException.Code.NONODE.intValue() == rc) {
                // not found, looking for a readonly bookie
                // No watcher needed - using persistent recursive watch on parent node
                zk.getData(pathAsReadonly, null,
                        (int rc2, String path2, Object o2, byte[] bytes2, Stat stat2) -> {
                    if (KeeperException.Code.OK.intValue() == rc2) {
                        try {
                            BookieServiceInfo bookieServiceInfo = deserializeBookieServiceInfo(bookieId, bytes2);
                            Versioned<BookieServiceInfo> result =
                                    new Versioned<>(bookieServiceInfo, new LongVersion(stat2.getCversion()));
                            log.info("Update BookieInfoCache (readonly bookie) {} -> {}", bookieId, result.getValue());
                            bookieServiceInfoCache.put(bookieId, result);
                            promise.complete(result);
                        } catch (IOException ex) {
                            log.error("Cannot update BookieInfo for ", ex);
                            promise.completeExceptionally(KeeperException.create(
                                    KeeperException.Code.DATAINCONSISTENCY, path2).initCause(ex));
                            return;
                        }
                    } else {
                        // not found as writable and readonly, the bookie is offline
                        promise.completeExceptionally(BKException.create(BKException.Code.NoBookieAvailableException));
                    }
                }, null);
            } else {
                promise.completeExceptionally(KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }, null);
        return promise;
    }

    @VisibleForTesting
    static BookieServiceInfo deserializeBookieServiceInfo(BookieId bookieId, byte[] bookieServiceInfo)
            throws IOException {
        if (bookieServiceInfo == null || bookieServiceInfo.length == 0) {
            return BookieServiceInfoUtils.buildLegacyBookieServiceInfo(bookieId.toString());
        }

        BookieServiceInfoFormat builder = BookieServiceInfoFormat.parseFrom(bookieServiceInfo);
        BookieServiceInfo bsi = new BookieServiceInfo();
        List<BookieServiceInfo.Endpoint> endpoints = builder.getEndpointsList().stream()
                .map(e -> {
                    BookieServiceInfo.Endpoint endpoint = new BookieServiceInfo.Endpoint();
                    endpoint.setId(e.getId());
                    endpoint.setPort(e.getPort());
                    endpoint.setHost(e.getHost());
                    endpoint.setProtocol(e.getProtocol());
                    endpoint.setAuth(e.getAuthList());
                    endpoint.setExtensions(e.getExtensionsList());
                    return endpoint;
                })
                .collect(Collectors.toList());

        bsi.setEndpoints(endpoints);
        bsi.setProperties(builder.getPropertiesMap());

        return bsi;
    }

    /**
     * Reads the list of bookies at the given path and eagerly caches the BookieServiceInfo
     * structure.
     *
     * @param regPath the path on ZooKeeper
     * @param watcher an optional watcher
     * @return an handle to the operation
     */
    private CompletableFuture<Versioned<Set<BookieId>>> getChildren(String regPath, Watcher watcher) {
        CompletableFuture<Versioned<Set<BookieId>>> future = FutureUtils.createFuture();
        zk.getChildren(regPath, watcher, (rc, path, ctx, children, stat) -> {
            if (KeeperException.Code.OK.intValue() != rc) {
                ZKException zke = new ZKException(KeeperException.create(KeeperException.Code.get(rc), path));
                future.completeExceptionally(zke.fillInStackTrace());
                return;
            }

            Version version = new LongVersion(stat.getCversion());
            Set<BookieId> bookies = convertToBookieAddresses(children);
            List<CompletableFuture<Versioned<BookieServiceInfo>>> bookieInfoUpdated = new ArrayList<>(bookies.size());
            for (BookieId id : bookies) {
                // update the cache for new bookies
                if (!bookieServiceInfoCache.containsKey(id)) {
                    bookieInfoUpdated.add(readBookieServiceInfoAsync(id));
                }
            }
            if (bookieInfoUpdated.isEmpty()) {
                future.complete(new Versioned<>(bookies, version));
            } else {
                FutureUtils
                        .collect(bookieInfoUpdated)
                        .whenComplete((List<Versioned<BookieServiceInfo>> info, Throwable error) -> {
                            // we are ignoring errors intentionally
                            // there could be bookies that publish unparsable information
                            // or other temporary/permanent errors
                            future.complete(new Versioned<>(bookies, version));
                        });
            }
        }, null);
        return future;
    }


    @Override
    public synchronized CompletableFuture<Void> watchWritableBookies(RegistrationListener listener) {
        CompletableFuture<Void> f;
        if (null == watchWritableBookiesTask) {
            f = new CompletableFuture<>();
            watchWritableBookiesTask = new WatchTask(bookieRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchWritableBookies(listener);
                }
            });
        } else {
            f = watchWritableBookiesTask.firstRunFuture;
        }

        watchWritableBookiesTask.addListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 1) {
            watchWritableBookiesTask.watch();
            // Establish persistent recursive watch when first listener is added
            if (bookieAddressTracking && persistentWatchCount < 2) {
                establishPersistentRecursiveWatch();
            }
        }
        return f;
    }

    @Override
    public synchronized void unwatchWritableBookies(RegistrationListener listener) {
        if (null == watchWritableBookiesTask) {
            return;
        }

        watchWritableBookiesTask.removeListener(listener);
        if (watchWritableBookiesTask.getNumListeners() == 0) {
            watchWritableBookiesTask.close();
            watchWritableBookiesTask = null;
        }
    }

    @Override
    public synchronized CompletableFuture<Void> watchReadOnlyBookies(RegistrationListener listener) {
        CompletableFuture<Void> f;
        if (null == watchReadOnlyBookiesTask) {
            f = new CompletableFuture<>();
            watchReadOnlyBookiesTask = new WatchTask(bookieReadonlyRegistrationPath, f);
            f = f.whenComplete((value, cause) -> {
                if (null != cause) {
                    unwatchReadOnlyBookies(listener);
                }
            });
        } else {
            f = watchReadOnlyBookiesTask.firstRunFuture;
        }

        watchReadOnlyBookiesTask.addListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 1) {
            watchReadOnlyBookiesTask.watch();
            // Establish persistent recursive watch when first listener is added
            if (bookieAddressTracking && persistentWatchCount < 2) {
                establishPersistentRecursiveWatch();
            }
        }
        return f;
    }

    @Override
    public synchronized void unwatchReadOnlyBookies(RegistrationListener listener) {
        if (null == watchReadOnlyBookiesTask) {
            return;
        }

        watchReadOnlyBookiesTask.removeListener(listener);
        if (watchReadOnlyBookiesTask.getNumListeners() == 0) {
            watchReadOnlyBookiesTask.close();
            watchReadOnlyBookiesTask = null;
        }
    }

    private static HashSet<BookieId> convertToBookieAddresses(List<String> children) {
        // Read the bookie addresses into a set for efficient lookup
        HashSet<BookieId> newBookieAddrs = Sets.newHashSet();
        for (String bookieAddrString : children) {
            if (READONLY.equals(bookieAddrString)) {
                continue;
            }
            BookieId bookieAddr = BookieId.parse(bookieAddrString);
            newBookieAddrs.add(bookieAddr);
        }
        return newBookieAddrs;
    }

    private static BookieId stripBookieIdFromPath(String path) {
        if (path == null) {
            return null;
        }
        final int slash = path.lastIndexOf('/');
        if (slash >= 0) {
            try {
                return BookieId.parse(path.substring(slash + 1));
            } catch (IllegalArgumentException e) {
                log.warn("Cannot decode bookieId from {}", path, e);
            }
        }
        return null;
    }

    /**
     * Persistent recursive watcher that handles all events for bookie nodes.
     * This watcher is set once on the parent node and automatically monitors
     * all child nodes (existing and future) for creation, deletion, and data changes.
     * This achieves O(1) watch complexity instead of O(N^2).
     */
    private class PersistentRecursiveWatcher implements Watcher {
        
        @Override
        public void process(WatchedEvent event) {
            if (log.isDebugEnabled()) {
                log.debug("Persistent recursive watch event: type={}, path={}, state={}", 
                    event.getType(), event.getPath(), event.getState());
            }
            
            // Handle session state changes
            if (event.getType() == EventType.None) {
                if (event.getState() == KeeperState.Expired) {
                    log.info("ZK session expired, invalidating cache and re-establishing watch");
                    bookieServiceInfoCache.clear();
                    persistentWatchCount = 0;
                    // Re-establish watch when session is reconnected
                    scheduleReestablishWatch();
                }
                return;
            }
            
            String path = event.getPath();
            if (path == null) {
                return;
            }
            
            // Handle events for bookie nodes
            BookieId bookieId = stripBookieIdFromPath(path);
            if (bookieId == null) {
                // Not a bookie node, might be the parent node itself
                if (path.equals(bookieRegistrationPath) || path.equals(bookieReadonlyRegistrationPath)) {
                    // Parent node children changed, trigger WatchTask to refresh list
                    handleParentNodeChildrenChanged(path);
                }
                return;
            }
            
            // Process bookie node events
            switch (event.getType()) {
                case NodeCreated:
                    log.info("Bookie node created: {}", bookieId);
                    // New bookie appeared, read its data
                    readBookieServiceInfoAsync(bookieId).whenComplete((info, throwable) -> {
                        if (throwable != null && log.isDebugEnabled()) {
                            log.debug("Failed to read bookie info for new bookie {}: {}", 
                                bookieId, throwable.getMessage());
                        }
                    });
                    // Trigger WatchTask to refresh bookie list
                    handleBookieListChange();
                    break;
                    
                case NodeDeleted:
                    log.info("Bookie node deleted: {}", bookieId);
                    // Bookie removed, invalidate cache
                    bookieServiceInfoCache.remove(bookieId);
                    // Trigger WatchTask to refresh bookie list
                    handleBookieListChange();
                    break;
                    
                case NodeDataChanged:
                    log.info("Bookie node data changed: {}", bookieId);
                    // Bookie data changed, refresh cache
                    readBookieServiceInfoAsync(bookieId).whenComplete((info, throwable) -> {
                        if (throwable != null && log.isDebugEnabled()) {
                            log.debug("Failed to refresh bookie info for {}: {}", 
                                bookieId, throwable.getMessage());
                        }
                    });
                    break;
                    
                default:
                    if (log.isDebugEnabled()) {
                        log.debug("Ignoring persistent recursive watch event type {} for {}", 
                            event.getType(), bookieId);
                    }
                    break;
            }
        }
        
        private void handleParentNodeChildrenChanged(String parentPath) {
            // Trigger WatchTask to refresh the bookie list
            scheduler.execute(() -> {
                if (parentPath.equals(bookieRegistrationPath) && watchWritableBookiesTask != null) {
                    watchWritableBookiesTask.run();
                } else if (parentPath.equals(bookieReadonlyRegistrationPath) 
                        && watchReadOnlyBookiesTask != null) {
                    watchReadOnlyBookiesTask.run();
                }
            });
        }
        
        private void handleBookieListChange() {
            // Trigger both WatchTasks to refresh bookie lists
            scheduler.execute(() -> {
                if (watchWritableBookiesTask != null) {
                    watchWritableBookiesTask.run();
                }
                if (watchReadOnlyBookiesTask != null) {
                    watchReadOnlyBookiesTask.run();
                }
            });
        }
        
        private void scheduleReestablishWatch() {
            scheduler.schedule(() -> {
                if (persistentWatchCount < 2 && bookieAddressTracking) {
                    establishPersistentRecursiveWatch();
                }
            }, ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }
    
    /**
     * Establish persistent recursive watch on parent nodes.
     * This watch will automatically monitor all child nodes for changes.
     */
    private synchronized void establishPersistentRecursiveWatch() {
        if (!bookieAddressTracking || persistentRecursiveWatcher == null || persistentWatchCount >= 2) {
            return;
        }
        
        try {
            // Set persistent recursive watch on writable bookies path (if not already set)
            if (persistentWatchCount == 0) {
                zk.addWatch(bookieRegistrationPath, persistentRecursiveWatcher, 
                    AddWatchMode.PERSISTENT_RECURSIVE, new AsyncCallback.VoidCallback() {
                        @Override
                        public void processResult(int rc, String path, Object ctx) {
                            synchronized (ZKRegistrationClient.this) {
                                if (rc == KeeperException.Code.OK.intValue()) {
                                    log.info("Persistent recursive watch established on {}", path);
                                    if (persistentWatchCount == 0) {
                                        persistentWatchCount = 1;
                                    }
                                    // Try to establish the second watch
                                    if (persistentWatchCount == 1) {
                                        establishReadOnlyWatch();
                                    }
                                } else {
                                    log.warn("Failed to establish persistent recursive watch on {}: {}", 
                                        path, KeeperException.Code.get(rc));
                                    // Retry after delay
                                    scheduler.schedule(() -> establishPersistentRecursiveWatch(), 
                                        ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
                                }
                            }
                        }
                    }, null);
            }
            
            // Set persistent recursive watch on readonly bookies path (if writable watch is already set)
            if (persistentWatchCount == 1) {
                establishReadOnlyWatch();
            }
        } catch (Exception e) {
            log.warn("Exception while establishing persistent recursive watch, will retry", e);
            scheduler.schedule(() -> establishPersistentRecursiveWatch(), 
                ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }
    
    private synchronized void establishReadOnlyWatch() {
        if (persistentWatchCount != 1) {
            return;
        }
        try {
            zk.addWatch(bookieReadonlyRegistrationPath, persistentRecursiveWatcher, 
                AddWatchMode.PERSISTENT_RECURSIVE, new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int rc, String path, Object ctx) {
                        synchronized (ZKRegistrationClient.this) {
                            if (rc == KeeperException.Code.OK.intValue()) {
                                log.info("Persistent recursive watch established on {}", path);
                                persistentWatchCount = 2;
                            } else {
                                log.warn("Failed to establish persistent recursive watch on {}: {}", 
                                    path, KeeperException.Code.get(rc));
                                // Retry after delay
                                scheduler.schedule(() -> establishPersistentRecursiveWatch(), 
                                    ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
                            }
                        }
                    }
                }, null);
        } catch (Exception e) {
            log.warn("Exception while establishing readonly persistent recursive watch, will retry", e);
            scheduler.schedule(() -> establishPersistentRecursiveWatch(), 
                ZK_CONNECT_BACKOFF_MS, TimeUnit.MILLISECONDS);
        }
    }

}
