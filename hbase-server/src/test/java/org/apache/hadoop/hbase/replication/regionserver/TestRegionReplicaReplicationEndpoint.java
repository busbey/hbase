/**
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
package org.apache.hadoop.hbase.replication.regionserver;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.Waiter;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hadoop.hbase.wal.WALKey;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.replication.ReplicationException;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.testclassification.FlakeyTests;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.util.ServerRegionReplicaUtil;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.log4j.Level;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.google.common.collect.Lists;

/**
 * Tests RegionReplicaReplicationEndpoint class by setting up region replicas and verifying
 * async wal replication replays the edits to the secondary region in various scenarios.
 */
@Category({FlakeyTests.class, MediumTests.class})
public class TestRegionReplicaReplicationEndpoint {

  private static final Log LOG = LogFactory.getLog(TestRegionReplicaReplicationEndpoint.class);

  static {
    ((Log4JLogger)RpcRetryingCaller.LOG).getLogger().setLevel(Level.ALL);
  }

  private static final int NB_SERVERS = 2;

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  @BeforeClass
  public static void beforeClass() throws Exception {
    /*
    Configuration conf = HTU.getConfiguration();
    conf.setFloat("hbase.regionserver.logroll.multiplier", 0.0003f);
    conf.setInt("replication.source.size.capacity", 10240);
    conf.setLong("replication.source.sleepforretries", 100);
    conf.setInt("hbase.regionserver.maxlogs", 10);
    conf.setLong("hbase.master.logcleaner.ttl", 10);
    conf.setInt("zookeeper.recovery.retry", 1);
    conf.setInt("zookeeper.recovery.retry.intervalmill", 10);
    conf.setBoolean(HConstants.REPLICATION_ENABLE_KEY, true);
    conf.setBoolean(ServerRegionReplicaUtil.REGION_REPLICA_REPLICATION_CONF_KEY, true);
    conf.setLong(HConstants.THREAD_WAKE_FREQUENCY, 100);
    conf.setInt("replication.stats.thread.period.seconds", 5);
    conf.setBoolean("hbase.tests.use.shortcircuit.reads", false);
    conf.setInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER, 3); // less number of retries is needed
    conf.setInt("hbase.client.serverside.retries.multiplier", 1);

    HTU.startMiniCluster(NB_SERVERS);*/
  }

  @AfterClass
  public static void afterClass() throws Exception {
    /*
    HTU.shutdownMiniCluster();
    */
  }

  @Ignore("To be fixed before 1.0")
  @Test
  public void testRegionReplicaReplicationPeerIsCreated() throws IOException, ReplicationException {
    // create a table with region replicas. Check whether the replication peer is created
    // and replication started.
    ReplicationAdmin admin = new ReplicationAdmin(HTU.getConfiguration());
    String peerId = "region_replica_replication";

    if (admin.getPeerConfig(peerId) != null) {
      admin.removePeer(peerId);
    }

    HTableDescriptor htd = HTU.createTableDescriptor(
      "testReplicationPeerIsCreated_no_region_replicas");
    HTU.getHBaseAdmin().createTable(htd);
    ReplicationPeerConfig peerConfig = admin.getPeerConfig(peerId);
    assertNull(peerConfig);

    htd = HTU.createTableDescriptor("testReplicationPeerIsCreated");
    htd.setRegionReplication(2);
    HTU.getHBaseAdmin().createTable(htd);

    // assert peer configuration is correct
    peerConfig = admin.getPeerConfig(peerId);
    assertNotNull(peerConfig);
    assertEquals(peerConfig.getClusterKey(), ZKUtil.getZooKeeperClusterKey(HTU.getConfiguration()));
    assertEquals(peerConfig.getReplicationEndpointImpl(),
      RegionReplicaReplicationEndpoint.class.getName());
    admin.close();
  }


  public void testRegionReplicaReplication(int regionReplication) throws Exception {
    // test region replica replication. Create a table with single region, write some data
    // ensure that data is replicated to the secondary region
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationWithReplicas_"
        + regionReplication);
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.toString());
    htd.setRegionReplication(regionReplication);
    HTU.getHBaseAdmin().createTable(htd);
    TableName tableNameNoReplicas =
        TableName.valueOf("testRegionReplicaReplicationWithReplicas_NO_REPLICAS");
    HTU.deleteTableIfAny(tableNameNoReplicas);
    HTU.createTable(tableNameNoReplicas, HBaseTestingUtility.fam1);

    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    Table tableNoReplicas = connection.getTable(tableNameNoReplicas);

    try {
      // load some data to the non-replicated table
      HTU.loadNumericRows(tableNoReplicas, HBaseTestingUtility.fam1, 6000, 7000);

      // load the data to the table
      HTU.loadNumericRows(table, HBaseTestingUtility.fam1, 0, 1000);

      verifyReplication(tableName, regionReplication, 0, 6000);

    } finally {
      table.close();
      tableNoReplicas.close();
      HTU.deleteTableIfAny(tableNameNoReplicas);
      connection.close();
    }
  }

  private void verifyReplication(TableName tableName, int regionReplication,
      final int startRow, final int endRow) throws Exception {
    // find the regions
    final HRegion[] regions = new HRegion[regionReplication];

    for (int i=0; i < NB_SERVERS; i++) {
      HRegionServer rs = HTU.getMiniHBaseCluster().getRegionServer(i);
      List<HRegion> onlineRegions = rs.getOnlineRegions(tableName);
      for (HRegion region : onlineRegions) {
        regions[region.getRegionInfo().getReplicaId()] = region;
      }
    }

    for (HRegion region : regions) {
      assertNotNull(region);
    }

    for (int i = 1; i < regionReplication; i++) {
      final HRegion region = regions[i];
      // wait until all the data is replicated to all secondary regions
      Waiter.waitFor(HTU.getConfiguration(), 60000, new Waiter.Predicate<Exception>() {
        @Override
        public boolean evaluate() throws Exception {
          LOG.info("verifying replication for region replica:" + region.getRegionInfo());
          try {
            HTU.verifyNumericRows(region, HBaseTestingUtility.fam1, startRow, endRow);
          } catch(Throwable ex) {
            LOG.warn("Verification from secondary region is not complete yet. Got:" + ex
              + " " + ex.getMessage());
            // still wait
            return false;
          }
          return true;
        }
      });
    }
  }

  @Ignore("To be fixed before 1.0")
  @Test(timeout = 60000)
  public void testRegionReplicaReplicationWith2Replicas() throws Exception {
    testRegionReplicaReplication(2);
  }

  @Ignore("To be fixed before 1.0")
  @Test(timeout = 60000)
  public void testRegionReplicaReplicationWith3Replicas() throws Exception {
    testRegionReplicaReplication(3);
  }

  @Ignore("To be fixed before 1.0")
  @Test(timeout = 60000)
  public void testRegionReplicaReplicationWith10Replicas() throws Exception {
    testRegionReplicaReplication(10);
  }

  @Ignore("To be fixed before 1.0")
  @Test (timeout = 60000)
  public void testRegionReplicaReplicationForFlushAndCompaction() throws Exception {
    // Tests a table with region replication 3. Writes some data, and causes flushes and
    // compactions. Verifies that the data is readable from the replicas. Note that this
    // does not test whether the replicas actually pick up flushed files and apply compaction
    // to their stores
    int regionReplication = 3;
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationForFlushAndCompaction");
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.toString());
    htd.setRegionReplication(regionReplication);
    HTU.getHBaseAdmin().createTable(htd);


    Connection connection = ConnectionFactory.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);

    try {
      // load the data to the table

      for (int i = 0; i < 6000; i += 1000) {
        LOG.info("Writing data from " + i + " to " + (i+1000));
        HTU.loadNumericRows(table, HBaseTestingUtility.fam1, i, i+1000);
        LOG.info("flushing table");
        HTU.flush(tableName);
        LOG.info("compacting table");
        HTU.compact(tableName, false);
      }

      verifyReplication(tableName, regionReplication, 0, 6000);
    } finally {
      table.close();
      connection.close();
    }
  }

  @Ignore("To be fixed before 1.0")
  @Test (timeout = 60000)
  public void testRegionReplicaReplicationIgnoresDisabledTables() throws Exception {
    testRegionReplicaReplicationIgnoresDisabledTables(false);
  }

  @Ignore("To be fixed before 1.0")
  @Test (timeout = 60000)
  public void testRegionReplicaReplicationIgnoresDroppedTables() throws Exception {
    testRegionReplicaReplicationIgnoresDisabledTables(true);
  }

  public void testRegionReplicaReplicationIgnoresDisabledTables(boolean dropTable)
      throws Exception {
    // tests having edits from a disabled or dropped table is handled correctly by skipping those
    // entries and further edits after the edits from dropped/disabled table can be replicated
    // without problems.
    TableName tableName = TableName.valueOf("testRegionReplicaReplicationIgnoresDisabledTables"
      + dropTable);
    HTableDescriptor htd = HTU.createTableDescriptor(tableName.toString());
    int regionReplication = 3;
    htd.setRegionReplication(regionReplication);
    HTU.deleteTableIfAny(tableName);
    HTU.getHBaseAdmin().createTable(htd);
    TableName toBeDisabledTable = TableName.valueOf(dropTable ? "droppedTable" : "disabledTable");
    HTU.deleteTableIfAny(toBeDisabledTable);
    htd = HTU.createTableDescriptor(toBeDisabledTable.toString());
    htd.setRegionReplication(regionReplication);
    HTU.getHBaseAdmin().createTable(htd);

    // both tables are created, now pause replication
    ReplicationAdmin admin = new ReplicationAdmin(HTU.getConfiguration());
    admin.disablePeer(ServerRegionReplicaUtil.getReplicationPeerId());

    // now that the replication is disabled, write to the table to be dropped, then drop the table.

    HConnection connection = HConnectionManager.createConnection(HTU.getConfiguration());
    Table table = connection.getTable(tableName);
    Table tableToBeDisabled = connection.getTable(toBeDisabledTable);

    HTU.loadNumericRows(tableToBeDisabled, HBaseTestingUtility.fam1, 6000, 7000);

    AtomicLong skippedEdits = new AtomicLong();
    RegionReplicaReplicationEndpoint.RegionReplicaOutputSink sink =
        mock(RegionReplicaReplicationEndpoint.RegionReplicaOutputSink.class);
    when(sink.getSkippedEditsCounter()).thenReturn(skippedEdits);
    RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter sinkWriter =
        new RegionReplicaReplicationEndpoint.RegionReplicaSinkWriter(sink,
          (ClusterConnection) connection,
          Executors.newSingleThreadExecutor(), 1000);

    HRegionLocation hrl = connection.locateRegion(toBeDisabledTable, HConstants.EMPTY_BYTE_ARRAY);
    byte[] encodedRegionName = hrl.getRegionInfo().getEncodedNameAsBytes();

    Entry entry = new Entry(
      new WALKey(encodedRegionName, toBeDisabledTable, 1),
      new WALEdit());

    HTU.getHBaseAdmin().disableTable(toBeDisabledTable); // disable the table
    if (dropTable) {
      HTU.getHBaseAdmin().deleteTable(toBeDisabledTable);
    }

    sinkWriter.append(toBeDisabledTable, encodedRegionName,
      HConstants.EMPTY_BYTE_ARRAY, Lists.newArrayList(entry, entry));

    assertEquals(2, skippedEdits.get());

    try {
      // load some data to the to-be-dropped table

      // load the data to the table
      HTU.loadNumericRows(table, HBaseTestingUtility.fam1, 0, 1000);

      // now enable the replication
      admin.enablePeer(ServerRegionReplicaUtil.getReplicationPeerId());

      verifyReplication(tableName, regionReplication, 0, 6000);

    } finally {
      admin.close();
      table.close();
      tableToBeDisabled.close();
      HTU.deleteTableIfAny(toBeDisabledTable);
      connection.close();
    }
  }
}
