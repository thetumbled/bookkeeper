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
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import org.apache.zookeeper.AsyncCallback.Children2Callback;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.common.testing.executors.MockExecutorController;
import org.apache.bookkeeper.discover.RegistrationClient.RegistrationListener;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.versioning.Versioned;
import org.apache.bookkeeper.zookeeper.MockZooKeeperTestCase;
import org.apache.zookeeper.AsyncCallback.VoidCallback;
import org.apache.zookeeper.AddWatchMode;
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
 * Unit test for persistent recursive watch feature in {@link ZKRegistrationClient}.
 */
@RunWith(MockitoJUnitRunner.Silent.class)
@Slf4j
public class TestZkRegistrationClientPersistentRecursiveWatch extends MockZooKeeperTestCase {

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
        this.zkRegistrationClient = new ZKRegistrationClient(
            mockZk,
            ledgersPath,
            mockExecutor,
            true // Enable bookie address tracking
        );
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
    public void testPersistentRecursiveWatchEstablished() throws Exception {
        // Mock getChildren for initial watch setup
        Set<BookieId> addresses = prepareNBookies(5);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);

        // Mock addWatch for persistent recursive watch
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        // Register a listener to trigger watch establishment
        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));

        // Verify persistent recursive watch was established
        verify(mockZk, times(1)).addWatch(
            eq(regPath),
            any(Watcher.class),
            eq(AddWatchMode.PERSISTENT_RECURSIVE),
            any(VoidCallback.class),
            any());
        verify(mockZk, times(1)).addWatch(
            eq(regReadonlyPath),
            any(Watcher.class),
            eq(AddWatchMode.PERSISTENT_RECURSIVE),
            any(VoidCallback.class),
            any());
    }

    @Test
    public void testNodeCreatedEvent() throws Exception {
        // Setup initial bookies
        Set<BookieId> initialAddresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : initialAddresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();
        assertEquals(3, initialUpdate.getValue().size());
        assertNotNull(initialUpdate);

        // Create a new bookie node
        BookieId newBookie = new BookieSocketAddress("127.0.0.1", 3190).toBookieId();
        prepareReadBookieServiceInfo(newBookie, false);

        // Simulate NodeCreated event via persistent recursive watch
        notifyPersistentRecursiveWatcher(
            EventType.NodeCreated,
            KeeperState.SyncConnected,
            regPath + "/" + newBookie);

        // Wait for the watch task to process the event
        controller.advance(Duration.ofMillis(100));

        // Verify that getChildren was called again to refresh the list
        verify(mockZk, times(2)).getChildren(
            eq(regPath),
            any(Watcher.class),
            any(Children2Callback.class),
            any());
    }

    @Test
    public void testNodeDeletedEvent() throws Exception {
        // Setup initial bookies
        Set<BookieId> addresses = prepareNBookies(5);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, false);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regPath, true,
            KeeperException.Code.OK.intValue(), children, stat);
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();
        assertEquals(5, initialUpdate.getValue().size());

        // Delete a bookie node
        BookieId deletedBookie = addresses.iterator().next();

        // Simulate NodeDeleted event via persistent recursive watch
        notifyPersistentRecursiveWatcher(
            EventType.NodeDeleted,
            KeeperState.SyncConnected,
            regPath + "/" + deletedBookie);

        // Wait for the watch task to process the event
        controller.advance(Duration.ofMillis(100));

        // Verify that getChildren was called again to refresh the list
        verify(mockZk, times(2)).getChildren(
            eq(regPath),
            any(Watcher.class),
            any(Children2Callback.class),
            any());
    }

    @Test
    public void testNodeDataChangedEvent() throws Exception {
        // Setup initial bookies
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
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();

        // Change bookie data - use empty byte array which will be treated as legacy format
        BookieId changedBookie = addresses.iterator().next();
        Stat newStat = mock(Stat.class);
        when(newStat.getCversion()).thenReturn(2);
        mockZkGetData(regPath + "/" + changedBookie.toString(),
                    false,
                    KeeperException.Code.OK.intValue(),
                    new byte[] {}, // Empty array will be treated as legacy bookie service info
                    newStat);

        // Simulate NodeDataChanged event via persistent recursive watch
        notifyPersistentRecursiveWatcher(
            EventType.NodeDataChanged,
            KeeperState.SyncConnected,
            regPath + "/" + changedBookie);

        // Wait for the data to be refreshed
        controller.advance(Duration.ofMillis(100));

        // Verify that getData was called to refresh the bookie info
        verify(mockZk, times(2)).getData(
            eq(regPath + "/" + changedBookie.toString()),
            eq(null),
            any(),
            any());
    }

    @Test
    public void testSessionExpired() throws Exception {
        // Setup initial bookies
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
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();

        // Simulate session expired
        notifyPersistentRecursiveWatcher(
            EventType.None,
            KeeperState.Expired,
            regPath);

        // Wait for re-establishment
        controller.advance(Duration.ofMillis(ZKRegistrationClient.ZK_CONNECT_BACKOFF_MS));

        // Verify that addWatch was called again to re-establish the watch
        verify(mockZk, times(2)).addWatch(
            eq(regPath),
            any(Watcher.class),
            eq(AddWatchMode.PERSISTENT_RECURSIVE),
            any(VoidCallback.class),
            any());
    }

    @Test
    public void testMultipleBookieChanges() throws Exception {
        // Setup initial bookies
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
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchWritableBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();
        assertEquals(3, initialUpdate.getValue().size());

        // Simulate multiple bookie changes
        for (BookieId bookie : addresses) {
            notifyPersistentRecursiveWatcher(
                EventType.NodeDataChanged,
                KeeperState.SyncConnected,
                regPath + "/" + bookie);
        }

        // Wait for processing
        controller.advance(Duration.ofMillis(100));

        // Verify that getData was called for each changed bookie
        for (BookieId bookie : addresses) {
            verify(mockZk, times(2)).getData(
                eq(regPath + "/" + bookie.toString()),
                eq(null),
                any(),
                any());
        }
    }

    @Test
    public void testReadOnlyBookieEvents() throws Exception {
        // Setup initial readonly bookies
        Set<BookieId> addresses = prepareNBookies(3);
        List<String> children = Lists.newArrayList();
        for (BookieId address : addresses) {
            children.add(address.toString());
            prepareReadBookieServiceInfo(address, true);
        }
        Stat stat = mock(Stat.class);
        when(stat.getCversion()).thenReturn(1234);

        mockGetChildren(regReadonlyPath, true,
            KeeperException.Code.OK.intValue(), children, stat);
        mockAddWatch(regPath, KeeperException.Code.OK.intValue());
        mockAddWatch(regReadonlyPath, KeeperException.Code.OK.intValue());

        LinkedBlockingQueue<Versioned<Set<BookieId>>> updates = new LinkedBlockingQueue<>();
        RegistrationListener listener = bookies -> {
            try {
                updates.put(bookies);
            } catch (InterruptedException e) {
                log.warn("Interrupted on enqueue bookie updates", e);
            }
        };

        result(zkRegistrationClient.watchReadOnlyBookies(listener));
        Versioned<Set<BookieId>> initialUpdate = updates.take();
        assertEquals(3, initialUpdate.getValue().size());

        // Simulate readonly bookie data change - use empty byte array which will be treated as legacy format
        BookieId changedBookie = addresses.iterator().next();
        Stat newStat = mock(Stat.class);
        when(newStat.getCversion()).thenReturn(2);
        mockZkGetData(regReadonlyPath + "/" + changedBookie,
                    false,
                    KeeperException.Code.OK.intValue(),
                    new byte[] {}, // Empty array will be treated as legacy bookie service info
                    newStat);

        notifyPersistentRecursiveWatcher(
            EventType.NodeDataChanged,
            KeeperState.SyncConnected,
            regReadonlyPath + "/" + changedBookie);

        controller.advance(Duration.ofMillis(100));

        // Verify that getData was called for the readonly bookie
        verify(mockZk, times(2)).getData(
            eq(regReadonlyPath + "/" + changedBookie),
            eq(null),
            any(),
            any());
    }
}

