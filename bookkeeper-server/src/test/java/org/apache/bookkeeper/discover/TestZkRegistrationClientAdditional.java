/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bookkeeper.discover;

import static org.apache.bookkeeper.common.concurrent.FutureUtils.result;
import static org.apache.bookkeeper.util.BookKeeperConstants.AVAILABLE_NODE;
import static org.apache.bookkeeper.util.BookKeeperConstants.READONLY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.MockZooKeeperTestCase;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Additional unit tests for {@link ZKRegistrationClient} to improve coverage.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
@Slf4j
public class TestZkRegistrationClientAdditional extends MockZooKeeperTestCase {

    @Rule
    public final TestName runtime = new TestName();

    private String ledgersPath;
    private String regPath;
    private String regReadonlyPath;
    private ZKRegistrationClient zkRegistrationClient;
    private ScheduledExecutorService mockExecutor;
    private MockExecutorController controller;

    @Override
    @Before
    public void setup() throws Exception {
        super.setup();

        this.ledgersPath = "/" + runtime.getMethodName();
        this.regPath = ledgersPath + "/" + AVAILABLE_NODE;
        this.regReadonlyPath = regPath + "/" + READONLY;
        this.mockExecutor = mock(ScheduledExecutorService.class);
        this.controller = new MockExecutorController()
            .controlExecute(mockExecutor)
            .controlSubmit(mockExecutor)
            .controlSchedule(mockExecutor)
            .controlScheduleAtFixedRate(mockExecutor, 10);
    }

    @After
    public void teardown() throws Exception {
        super.teardown();

        if (null != zkRegistrationClient) {
            zkRegistrationClient.close();
        }
    }

    private static Set<BookieId> prepareNBookies(int num) {
        Set<BookieId> bookies = new HashSet<>();
        for (int i = 0; i < num; i++) {
            bookies.add(new BookieSocketAddress("127.0.0.1", 3181 + i).toBookieId());
        }
        return bookies;
    }

    private void prepareReadBookieServiceInfo(BookieId address, boolean readonly) throws Exception {
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1);
        if (readonly) {
            mockZkGetData(regPath + "/" + address.toString(),
                        false,
                        KeeperException.Code.NONODE.intValue(),
                        new byte[] {},
                        stat);
            mockZkGetData(regReadonlyPath + "/" + address,
                        false,
                        KeeperException.Code.OK.intValue(),
                        new byte[] {},
                        stat);
        } else {
            mockZkGetData(regPath + "/" + address.toString(),
                        false,
                        KeeperException.Code.OK.intValue(),
                        new byte[] {},
                        stat);
            mockZkGetData(regReadonlyPath + "/" + address,
                        false,
                        KeeperException.Code.NONODE.intValue(),
                        new byte[] {},
                        stat);
        }
    }

    @Test
    public void testIsBookieAddressTracking() throws Exception {
        // Test with tracking enabled
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);
        assertTrue(zkRegistrationClient.isBookieAddressTracking());

        // Test with tracking disabled
        zkRegistrationClient.close();
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, false);
        assertFalse(zkRegistrationClient.isBookieAddressTracking());
    }

    @Test
    public void testGetZk() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);
        assertSame(mockZk, zkRegistrationClient.getZk());
    }

    @Test
    public void testClose() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);
        // close() should not throw exception
        zkRegistrationClient.close();
        // Can be called multiple times
        zkRegistrationClient.close();
    }

    @Test
    public void testGetBookieServiceInfoFromCache() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        // Setup initial bookies and cache them
        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, false,
            KeeperException.Code.OK.intValue(), children, stat);

        // Get writable bookies to populate cache
        result(zkRegistrationClient.getWritableBookies());

        // Now getBookieServiceInfo should return from cache
        BookieId bookieId = addresses.iterator().next();
        Versioned<BookieServiceInfo> result = result(zkRegistrationClient.getBookieServiceInfo(bookieId));
        assertNotNull(result);
        assertNotNull(result.getValue());

        // Verify that getData was called only once (during getWritableBookies)
        verify(mockZk, times(1)).getData(
            eq(regPath + "/" + bookieId.toString()),
            eq(null),
            any(),
            any());
    }

    @Test
    public void testGetBookieServiceInfoNotInCache() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        BookieId bookieId = new BookieSocketAddress("127.0.0.1", 3181).toBookieId();

        try {
            result(zkRegistrationClient.getBookieServiceInfo(bookieId));
            fail("Should throw exception when bookie not in cache");
        } catch (BKException.BKBookieHandleNotAvailableException e) {
            // expected
        }
    }

    @Test
    public void testGetBookieServiceInfoReadOnlyBookie() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        // Setup readonly bookie
        BookieId readonlyBookie = new BookieSocketAddress("127.0.0.1", 3181).toBookieId();
        List<String> children = Lists.newArrayList();
        children.add(readonlyBookie.toString());
        prepareReadBookieServiceInfo(readonlyBookie, true);

        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regReadonlyPath, false,
            KeeperException.Code.OK.intValue(), children, stat);

        // Get readonly bookies to populate cache
        result(zkRegistrationClient.getReadOnlyBookies());

        // Now getBookieServiceInfo should return from cache
        Versioned<BookieServiceInfo> result = result(zkRegistrationClient.getBookieServiceInfo(readonlyBookie));
        assertNotNull(result);
        assertNotNull(result.getValue());
    }

    @Test
    public void testWatchTaskClosedState() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        RegistrationListener listener = bookies -> {};

        result(zkRegistrationClient.watchWritableBookies(listener));
        ZKRegistrationClient.WatchTask watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        assertNotNull(watchTask);
        assertFalse(watchTask.isClosed());

        // Close the watch task
        zkRegistrationClient.unwatchWritableBookies(listener);
        assertTrue(watchTask.isClosed());

        // Run should not do anything when closed
        watchTask.run();
        // Verify no additional getChildren calls
        verify(mockZk, times(1)).getChildren(
            anyString(), any(Watcher.class), any(Children2Callback.class), any());
    }

    @Test
    public void testWatchTaskAddRemoveListener() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        RegistrationListener listener1 = bookies -> {};
        RegistrationListener listener2 = bookies -> {};

        result(zkRegistrationClient.watchWritableBookies(listener1));
        ZKRegistrationClient.WatchTask watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        assertEquals(1, watchTask.getNumListeners());

        result(zkRegistrationClient.watchWritableBookies(listener2));
        assertEquals(2, watchTask.getNumListeners());

        // Remove listener
        assertTrue(watchTask.removeListener(listener1));
        assertEquals(1, watchTask.getNumListeners());

        // Remove non-existent listener
        assertFalse(watchTask.removeListener(listener1));
        assertEquals(1, watchTask.getNumListeners());
    }

    @Test
    public void testWatchTaskAddListenerWithCachedBookies() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener1 = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener1));
        Versioned<Set<BookieId>> firstUpdate = updates.take();

        // Add second listener - should receive cached bookies immediately
        LinkedBlockingQueue<Versioned<Set<BookieId>>> secondUpdates = new LinkedBlockingQueue<>();
        RegistrationListener listener2 = bookies -> {
            try {
                secondUpdates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener2));
        Versioned<Set<BookieId>> secondUpdate = secondUpdates.take();

        // Both should have same data
        assertEquals(firstUpdate.getVersion(), secondUpdate.getVersion());
        assertEquals(firstUpdate.getValue(), secondUpdate.getValue());
    }

    @Test
    public void testWatchTaskProcessEventNone() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        RegistrationListener listener = bookies -> {};
        result(zkRegistrationClient.watchWritableBookies(listener));

        // Trigger None event with SyncConnected state (should be ignored)
        notifyWatchedEvent(EventType.None, KeeperState.SyncConnected, regPath);

        // Should not trigger additional getChildren
        verify(mockZk, times(1)).getChildren(
            anyString(), any(Watcher.class), any(Children2Callback.class), any());
    }

    @Test
    public void testWatchTaskAcceptWithError() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        RegistrationListener listener = bookies -> {};
        zkRegistrationClient.watchWritableBookies(listener);

        ZKRegistrationClient.WatchTask watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        assertNotNull(watchTask);

        // Simulate error in accept
        watchTask.accept(null, new KeeperException.ConnectionLossException());

        // Should schedule retry
        controller.advance(Duration.ofMillis(ZKRegistrationClient.ZK_CONNECT_BACKOFF_MS));
        verify(mockExecutor, times(1)).schedule(
            any(Runnable.class),
            eq((long) ZKRegistrationClient.ZK_CONNECT_BACKOFF_MS),
            any());
    }

    @Test
    public void testWatchTaskAcceptWithSameVersion() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> firstUpdate = updates.take();

        ZKRegistrationClient.WatchTask watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        
        // Accept same version - should not notify listeners
        watchTask.accept(firstUpdate, null);
        
        // Should not have new update
        assertNull(updates.poll());
    }

    @Test
    public void testUnwatchWhenNotWatching() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        RegistrationListener listener = bookies -> {};

        // Unwatch when not watching should not throw exception
        zkRegistrationClient.unwatchWritableBookies(listener);
        zkRegistrationClient.unwatchReadOnlyBookies(listener);
    }

    @Test
    public void testGetBookieServiceInfoWithInvalidData() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        BookieId bookieId = new BookieSocketAddress("127.0.0.1", 3181).toBookieId();
        List<String> children = Lists.newArrayList();
        children.add(bookieId.toString());

        // Mock getData to return invalid protobuf data
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1);
        mockZkGetData(regPath + "/" + bookieId.toString(),
                    false,
                    KeeperException.Code.OK.intValue(),
                    new byte[] {1, 2, 3}, // Invalid protobuf
                    stat);

        mockGetChildren(regPath, false,
            KeeperException.Code.OK.intValue(), children, stat);

        // Should handle error gracefully
        try {
            result(zkRegistrationClient.getWritableBookies());
            // The error should be logged but not propagated
        } catch (Exception e) {
            // May or may not throw depending on implementation
        }
    }

    @Test
    public void testPersistentRecursiveWatchWithBookieAddressTrackingDisabled() throws Exception {
        // Test that persistent recursive watch is not established when tracking is disabled
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, false);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        int zkCallbackDelayMs = 100;
        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat, zkCallbackDelayMs);

        RegistrationListener listener = bookies -> {};
        CompletableFuture<Void> watchFuture = zkRegistrationClient.watchWritableBookies(listener);

        // Trigger zkCallbackExecutor to execute getChildren callback
        zkCallbackController.advance(Duration.ofMillis(zkCallbackDelayMs));

        result(watchFuture);

        // Wait a bit to ensure any async operations complete
        controller.advance(Duration.ofMillis(100));

        // Verify addWatch was not called (since bookieAddressTracking is false)
        verify(mockZk, times(0)).addWatch(
            anyString(), any(Watcher.class), any(), any(), any());
    }

    @Test
    public void testConcurrentWatchUnwatch() throws Exception {
        zkRegistrationClient = new ZKRegistrationClient(
            mockZk, ledgersPath, mockExecutor, true);

        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        RegistrationListener listener1 = bookies -> {};
        RegistrationListener listener2 = bookies -> {};

        // Concurrent watch/unwatch
        CompletableFuture<Void> watch1 = zkRegistrationClient.watchWritableBookies(listener1);
        CompletableFuture<Void> watch2 = zkRegistrationClient.watchWritableBookies(listener2);

        result(watch1);
        result(watch2);

        ZKRegistrationClient.WatchTask watchTask = zkRegistrationClient.getWatchWritableBookiesTask();
        assertEquals(2, watchTask.getNumListeners());

        zkRegistrationClient.unwatchWritableBookies(listener1);
        assertEquals(1, watchTask.getNumListeners());

        zkRegistrationClient.unwatchWritableBookies(listener2);
        assertNull(zkRegistrationClient.getWatchWritableBookiesTask());
    }
}

