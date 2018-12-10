/*
 * Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazon.janusgraph.graphdb.dynamodb;

import static org.apache.tinkerpop.gremlin.structure.Direction.BOTH;
import static org.apache.tinkerpop.gremlin.structure.Direction.OUT;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_BACKEND;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MAX_COMMIT_TIME;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.SYSTEM_LOG_TRANSACTIONS;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.TRANSACTION_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.USER_LOG;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.VERBOSE_TX_RECOVERY;
import static org.janusgraph.testutil.JanusGraphAssert.assertCount;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.log.Change;
import org.janusgraph.core.log.LogProcessorFramework;
import org.janusgraph.core.log.TransactionRecovery;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.log.Log;
import org.janusgraph.diskstorage.log.Message;
import org.janusgraph.diskstorage.log.MessageReader;
import org.janusgraph.diskstorage.log.ReadMarker;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.diskstorage.util.time.TimestampProvider;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.TestMockLog;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.log.LogTxMeta;
import org.janusgraph.graphdb.database.log.LogTxStatus;
import org.janusgraph.graphdb.database.log.TransactionLogHeader;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.log.StandardTransactionLogProcessor;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.testutils.CiHeartbeat;
import com.google.common.collect.Iterables;

/**
 *
 * FunctionalTitanGraphTest contains the specializations of the Titan functional tests required for
 * the DynamoDB storage backend.
 *
 * @author Alexander Patrikalakis
 * @author Johan Jacobs
 *
 */
public abstract class AbstractDynamoDBGraphTest extends JanusGraphTest {
    private final CiHeartbeat ciHeartbeat;

    @Rule
    public final TestName testName = new TestName();

    protected final BackendDataModel model;
    protected AbstractDynamoDBGraphTest(final BackendDataModel model) {
        this.model = model;
        this.ciHeartbeat = new CiHeartbeat();
    }

    @Override
    public WriteConfiguration getConfiguration() {
        final String methodName = testName.getMethodName();
        final List<String> extraStoreNames = methodName.contains("simpleLogTest") ? Collections.singletonList("ulog_test") : Collections.emptyList();
        return TestGraphUtil.instance.graphConfigWithClusterPartitionsAndExtraStores(model, extraStoreNames, 1 /*janusGraphClusterPartitions*/);
    }

    @Override
    public void testGotGIndexRemoval() throws InterruptedException, ExecutionException {
        clopen(option(LOG_SEND_DELAY, MANAGEMENT_LOG), Duration.ZERO,
                option(KCVSLog.LOG_READ_LAG_TIME, MANAGEMENT_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, MANAGEMENT_LOG), Duration.ofMillis(250)
        );

        final String name = "name";

        // Load Graph of the Gods
        GraphOfTheGodsFactory.loadWithoutMixedIndex(graph,
                false); // True makes the index on names unique.  Test fails when this is true.
        // Change to false and test will pass.
        newTx();
        finishSchema();

        JanusGraphIndex graphIndex = mgmt.getGraphIndex(name);

        // Sanity checks on the index that we assume GraphOfTheGodsFactory created
        assertNotNull(graphIndex);
        assertEquals(1, graphIndex.getFieldKeys().length);
        assertEquals(name, graphIndex.getFieldKeys()[0].name());
        assertEquals("internalindex", graphIndex.getBackingIndex());
        assertEquals(SchemaStatus.ENABLED, graphIndex.getIndexStatus(graphIndex.getFieldKeys()[0]));
        finishSchema();

        // Disable name index
        graphIndex = mgmt.getGraphIndex(name);
        mgmt.updateIndex(graphIndex, SchemaAction.DISABLE_INDEX);
        mgmt.commit();
        tx.commit();

        ManagementUtil.awaitGraphIndexUpdate(graph, name, 5, ChronoUnit.SECONDS);
        finishSchema();

        // Remove name index
        graphIndex = mgmt.getGraphIndex(name);
        mgmt.updateIndex(graphIndex, SchemaAction.REMOVE_INDEX);
        JanusGraphManagement.IndexJobFuture graphMetrics = mgmt.getIndexJobStatus(graphIndex);
        finishSchema();

        // Should have deleted at least one record
        assertNotEquals(0, graphMetrics.get().getCustom(IndexRemoveJob.DELETED_RECORDS_COUNT));
    }


    @Override
    public void simpleLogTest(final boolean withLogFailure) throws InterruptedException {
        final String userLogName = "test";
        final Serializer serializer = graph.getDataSerializer();
        final EdgeSerializer edgeSerializer = graph.getEdgeSerializer();
        final TimestampProvider times = graph.getConfiguration().getTimestampProvider();
        final Instant startTime = Instant.now();
        clopen(option(SYSTEM_LOG_TRANSACTIONS), true,
                option(LOG_BACKEND, USER_LOG), (withLogFailure ? TestMockLog.class.getName() : LOG_BACKEND.getDefaultValue()),
                option(TestMockLog.LOG_MOCK_FAILADD, USER_LOG), withLogFailure,
                option(KCVSLog.LOG_READ_LAG_TIME, USER_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, USER_LOG), Duration.ofMillis(250),
                option(LOG_SEND_DELAY, USER_LOG), Duration.ofMillis(100),
                option(KCVSLog.LOG_READ_LAG_TIME, TRANSACTION_LOG), Duration.ofMillis(50),
                option(LOG_READ_INTERVAL, TRANSACTION_LOG), Duration.ofMillis(250),
                option(MAX_COMMIT_TIME), Duration.ofSeconds(1)
        );
        final String instanceId = graph.getConfiguration().getUniqueGraphId();

        PropertyKey weight = tx.makePropertyKey("weight").dataType(Float.class).cardinality(Cardinality.SINGLE).make();
        EdgeLabel knows = tx.makeEdgeLabel("knows").make();
        JanusGraphVertex n1 = tx.addVertex("weight", 10.5);
        tx.addProperties(knows, weight);
        newTx();

        final Instant txTimes[] = new Instant[4];
        //Transaction with custom user log name
        txTimes[0] = times.getTime();
        JanusGraphTransaction tx2 = graph.buildTransaction().logIdentifier(userLogName).start();
        JanusGraphVertex v1 = tx2.addVertex("weight", 111.1);
        v1.addEdge("knows", v1);
        tx2.commit();
        final long v1id = getId(v1);
        txTimes[1] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userLogName).start();
        JanusGraphVertex v2 = tx2.addVertex("weight", 222.2);
        v2.addEdge("knows", getV(tx2, v1id));
        tx2.commit();
        final long v2id = getId(v2);
        //Only read tx
        tx2 = graph.buildTransaction().logIdentifier(userLogName).start();
        v1 = getV(tx2, v1id);
        assertEquals(111.1, v1.<Float>value("weight").doubleValue(), 0.01);
        assertEquals(222.2, getV(tx2, v2).<Float>value("weight").doubleValue(), 0.01);
        tx2.commit();
        //Deleting transaction
        txTimes[2] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userLogName).start();
        v2 = getV(tx2, v2id);
        assertEquals(222.2, v2.<Float>value("weight").doubleValue(), 0.01);
        v2.remove();
        tx2.commit();
        //Edge modifying transaction
        txTimes[3] = times.getTime();
        tx2 = graph.buildTransaction().logIdentifier(userLogName).start();
        v1 = getV(tx2, v1id);
        assertEquals(111.1, v1.<Float>value("weight").doubleValue(), 0.01);
        final Edge e = (Edge) Iterables
                .getOnlyElement(v1.query().direction(Direction.OUT).labels("knows").edges());
        assertFalse(e.property("weight").isPresent());
        e.property("weight", 44.4);
        tx2.commit();
        close();
        final Instant endTime = times.getTime();
        final ReadMarker startMarker = ReadMarker.fromTime(startTime);

        final Log transactionLog = openTxLog();
        final Log userLog = openUserLog(userLogName);
        final EnumMap<LogTxStatus, AtomicInteger> txMsgCounter = new EnumMap<>(LogTxStatus.class);
        for (final LogTxStatus status : LogTxStatus.values()) txMsgCounter.put(status, new AtomicInteger(0));
        final AtomicInteger userLogMeta = new AtomicInteger(0);
        transactionLog.registerReader(startMarker, new MessageReader() {
            @Override
            public void read(Message message) {
                final Instant msgTime = message.getTimestamp();
                assertTrue(msgTime.isAfter(startTime) || msgTime.equals(startTime));
                assertNotNull(message.getSenderId());
                final TransactionLogHeader.Entry txEntry = TransactionLogHeader.parse(message.getContent(), serializer, times);
                final TransactionLogHeader header = txEntry.getHeader();
//                    System.out.println(header.getTimestamp(TimeUnit.MILLISECONDS));
                assertTrue(header.getTimestamp().isAfter(startTime) || header.getTimestamp().equals(startTime));
                assertTrue(header.getTimestamp().isBefore(msgTime) || header.getTimestamp().equals(msgTime));
                assertNotNull(txEntry.getMetadata());
                assertNull(txEntry.getMetadata().get(LogTxMeta.GROUPNAME));
                final LogTxStatus status = txEntry.getStatus();
                if (status == LogTxStatus.PRECOMMIT) {
                    assertTrue(txEntry.hasContent());
                    final Object logId = txEntry.getMetadata().get(LogTxMeta.LOG_ID);
                    if (logId != null) {
                        assertTrue(logId instanceof String);
                        assertEquals(userLogName, logId);
                        userLogMeta.incrementAndGet();
                    }
                } else if (withLogFailure) {
                    assertTrue(status.isPrimarySuccess() || status == LogTxStatus.SECONDARY_FAILURE);
                    if (status == LogTxStatus.SECONDARY_FAILURE) {
                        final TransactionLogHeader.SecondaryFailures secFail = txEntry.getContentAsSecondaryFailures(serializer);
                        assertTrue(secFail.failedIndexes.isEmpty());
                        assertTrue(secFail.userLogFailure);
                    }
                } else {
                    assertFalse(txEntry.hasContent());
                    assertTrue(status.isSuccess());
                }
                txMsgCounter.get(txEntry.getStatus()).incrementAndGet();
            }

            @Override public void updateState() {}
        });
        final EnumMap<Change, AtomicInteger> userChangeCounter = new EnumMap<>(Change.class);
        for (final Change change : Change.values()) userChangeCounter.put(change, new AtomicInteger(0));
        final AtomicInteger userLogMsgCounter = new AtomicInteger(0);
        userLog.registerReader(startMarker, new MessageReader() {
            @Override
            public void read(Message message) {
                final Instant msgTime = message.getTimestamp();
                assertTrue(msgTime.isAfter(startTime) || msgTime.equals(startTime));
                assertNotNull(message.getSenderId());
                final StaticBuffer content = message.getContent();
                assertTrue(content != null && content.length() > 0);
                final TransactionLogHeader.Entry transactionEntry = TransactionLogHeader.parse(content, serializer, times);

                final Instant txTime = transactionEntry.getHeader().getTimestamp();
                assertTrue(txTime.isBefore(msgTime) || txTime.equals(msgTime));
                assertTrue(txTime.isAfter(startTime) || txTime.equals(msgTime));
                final long transactionId = transactionEntry.getHeader().getId();
                assertTrue(transactionId > 0);
                transactionEntry.getContentAsModifications(serializer).forEach(modification -> {
                    assertTrue(modification.state == Change.ADDED || modification.state == Change.REMOVED);
                    userChangeCounter.get(modification.state).incrementAndGet();
                });
                userLogMsgCounter.incrementAndGet();
            }

            @Override public void updateState() {}
        });
        Thread.sleep(10000);
        assertEquals(5, txMsgCounter.get(LogTxStatus.PRECOMMIT).get());
        assertEquals(4, txMsgCounter.get(LogTxStatus.PRIMARY_SUCCESS).get());
        assertEquals(1, txMsgCounter.get(LogTxStatus.COMPLETE_SUCCESS).get());
        assertEquals(4, userLogMeta.get());
        if (withLogFailure) assertEquals(4, txMsgCounter.get(LogTxStatus.SECONDARY_FAILURE).get());
        else assertEquals(4, txMsgCounter.get(LogTxStatus.SECONDARY_SUCCESS).get());
        //User-Log
        if (withLogFailure) {
            assertEquals(0, userLogMsgCounter.get());
        } else {
            assertEquals(4, userLogMsgCounter.get());
            assertEquals(7, userChangeCounter.get(Change.ADDED).get());
            assertEquals(4, userChangeCounter.get(Change.REMOVED).get());
        }

        clopen(option(VERBOSE_TX_RECOVERY), true);
        /*
        Transaction Recovery
         */
        final TransactionRecovery recovery = JanusGraphFactory.startTransactionRecovery(graph, startTime);


        /*
        Use user log processing framework
         */
        final AtomicInteger userLogCount = new AtomicInteger(0);
        final LogProcessorFramework userLogs = JanusGraphFactory.openTransactionLog(graph);
        userLogs.addLogProcessor(userLogName).setStartTime(startTime).setRetryAttempts(1)
                .addProcessor((tx, txId, changes) -> {
                    assertEquals(instanceId, txId.getInstanceId());
                    // Just some reasonable upper bound
                    assertTrue(txId.getTransactionId() > 0 && txId.getTransactionId() < 100);
                    final Instant txTime = txId.getTransactionTime();
                    // Times should be within a second
                    assertTrue(String.format("tx timestamp %s not between start %s and end time %s",
                                    txTime, startTime, endTime),
                            (txTime.isAfter(startTime) || txTime.equals(startTime))
                                    && (txTime.isBefore(endTime) || txTime.equals(endTime)));

                    assertTrue(tx.containsRelationType("knows"));
                    assertTrue(tx.containsRelationType("weight"));
                    final EdgeLabel knows1 = tx.getEdgeLabel("knows");
                    final PropertyKey weight1 = tx.getPropertyKey("weight");

                    Instant txTimeMicro = txId.getTransactionTime();

                    int txNo;
                    if (txTimeMicro.isBefore(txTimes[1])) {
                        txNo = 1;
                        //v1 addition transaction
                        assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                        assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                        assertEquals(1, Iterables.size(changes.getVertices(Change.ANY)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ADDED)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, knows1)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, weight1)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));
                        assertEquals(0, Iterables.size(changes.getRelations(Change.REMOVED)));

                        final JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                        assertEquals(v1id, getId(v));
                        final VertexProperty<Float> p
                                = Iterables.getOnlyElement(changes.getProperties(v, Change.ADDED, "weight"));
                        assertEquals(111.1, p.value().doubleValue(), 0.01);
                        assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                        assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                    } else if (txTimeMicro.isBefore(txTimes[2])) {
                        txNo = 2;
                        //v2 addition transaction
                        assertEquals(1, Iterables.size(changes.getVertices(Change.ADDED)));
                        assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                        assertEquals(2, Iterables.size(changes.getVertices(Change.ANY)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ADDED)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, knows1)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED, weight1)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));
                        assertEquals(0, Iterables.size(changes.getRelations(Change.REMOVED)));

                        final JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ADDED));
                        assertEquals(v2id, getId(v));
                        final VertexProperty<Float> p
                                = Iterables.getOnlyElement(changes.getProperties(v, Change.ADDED, "weight"));
                        assertEquals(222.2, p.value().doubleValue(), 0.01);
                        assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, OUT)));
                        assertEquals(1, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                    } else if (txTimeMicro.isBefore(txTimes[3])) {
                        txNo = 3;
                        //v2 deletion transaction
                        assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                        assertEquals(1, Iterables.size(changes.getVertices(Change.REMOVED)));
                        assertEquals(2, Iterables.size(changes.getVertices(Change.ANY)));
                        assertEquals(0, Iterables.size(changes.getRelations(Change.ADDED)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.REMOVED)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, knows1)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, weight1)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));

                        final JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.REMOVED));
                        assertEquals(v2id, getId(v));
                        final VertexProperty<Float> p
                                = Iterables.getOnlyElement(changes.getProperties(v, Change.REMOVED, "weight"));
                        assertEquals(222.2, p.value().doubleValue(), 0.01);
                        assertEquals(1, Iterables.size(changes.getEdges(v, Change.REMOVED, OUT)));
                        assertEquals(0, Iterables.size(changes.getEdges(v, Change.ADDED, BOTH)));
                    } else {
                        txNo = 4;
                        //v1 edge modification
                        assertEquals(0, Iterables.size(changes.getVertices(Change.ADDED)));
                        assertEquals(0, Iterables.size(changes.getVertices(Change.REMOVED)));
                        assertEquals(1, Iterables.size(changes.getVertices(Change.ANY)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.ADDED)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED)));
                        assertEquals(1, Iterables.size(changes.getRelations(Change.REMOVED, knows1)));
                        assertEquals(2, Iterables.size(changes.getRelations(Change.ANY)));

                        final JanusGraphVertex v = Iterables.getOnlyElement(changes.getVertices(Change.ANY));
                        assertEquals(v1id, getId(v));
                        JanusGraphEdge e1
                                = Iterables.getOnlyElement(changes.getEdges(v, Change.REMOVED, Direction.OUT, "knows"));
                        assertFalse(e1.property("weight").isPresent());
                        assertEquals(v, e1.vertex(Direction.IN));
                        e1 = Iterables.getOnlyElement(changes.getEdges(v, Change.ADDED, Direction.OUT, "knows"));
                        assertEquals(44.4, e1.<Float>value("weight").doubleValue(), 0.01);
                        assertEquals(v, e1.vertex(Direction.IN));
                    }

                    //See only current state of graph in transaction
                    final JanusGraphVertex v11 = getV(tx, v1id);
                    assertNotNull(v11);
                    assertTrue(v11.isLoaded());
                    if (txNo != 2) {
                        //In the transaction that adds v2, v2 will be considered "loaded"
                        assertMissing(tx, v2id);
//                    assertTrue(txNo + " - " + v2, v2 == null || v2.isRemoved());
                    }
                    assertEquals(111.1, v11.<Float>value("weight").doubleValue(), 0.01);
                    assertCount(1, v11.query().direction(Direction.OUT).edges());

                    userLogCount.incrementAndGet();
                }).build();

        //wait
        Thread.sleep(22000L);

        recovery.shutdown();
        long[] recoveryStats = ((StandardTransactionLogProcessor) recovery).getStatistics();
        if (withLogFailure) {
            assertEquals(1, recoveryStats[0]);
            assertEquals(4, recoveryStats[1]);
        } else {
            assertEquals(5, recoveryStats[0]);
            assertEquals(0, recoveryStats[1]);

        }

        userLogs.removeLogProcessor(userLogName);
        userLogs.shutdown();
        assertEquals(4, userLogCount.get());
    }

    @AfterClass
    public static void deleteTables() throws BackendException {
        TestGraphUtil.instance.cleanUpTables();
    }

    @Before
    public void setUpTest() throws Exception {
        this.ciHeartbeat.startHeartbeat(this.testName.getMethodName());
    }

    @After
    public void tearDownTest() throws Exception {
        this.ciHeartbeat.stopHeartbeat();
    }
}
