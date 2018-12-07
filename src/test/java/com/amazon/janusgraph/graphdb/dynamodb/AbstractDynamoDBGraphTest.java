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

import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_READ_INTERVAL;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.LOG_SEND_DELAY;
import static org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration.MANAGEMENT_LOG;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.util.ManagementUtil;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.configuration.WriteConfiguration;
import org.janusgraph.diskstorage.log.kcvs.KCVSLog;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.JanusGraphTest;
import org.janusgraph.graphdb.olap.job.IndexRemoveJob;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import com.amazon.janusgraph.TestGraphUtil;
import com.amazon.janusgraph.diskstorage.dynamodb.BackendDataModel;
import com.amazon.janusgraph.testutils.CiHeartbeat;

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
