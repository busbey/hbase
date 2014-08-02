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
package org.apache.hadoop.hbase.regionserver.wal;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.junit.Assert;
import static org.junit.Assert.assertTrue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegionServer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Tests for conditions that should trigger RegionServer aborts when
 * rolling the current HLog fails.
 */
@Category({RegionServerTests.class, MediumTests.class})
public class TestLogRollAbort {
  private static final Log LOG = LogFactory.getLog(TestLogRolling.class);
  private static MiniDFSCluster dfsCluster;
  private static Admin admin;
  private static MiniHBaseCluster cluster;
  private final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();

  /* For the split-then-roll test */
  private static final Path HBASEDIR = new Path("/hbase");
  private static final Path OLDLOGDIR = new Path(HBASEDIR, "hlog.old");

  // Need to override this setup so we can edit the config before it gets sent
  // to the HDFS & HBase cluster startup.
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Tweak default timeout values down for faster recovery
    TEST_UTIL.getConfiguration().setInt(
        "hbase.regionserver.logroll.errors.tolerated", 2);
    TEST_UTIL.getConfiguration().setInt("hbase.rpc.timeout", 10 * 1000);

    // Increase the amount of time between client retries
    TEST_UTIL.getConfiguration().setLong("hbase.client.pause", 5 * 1000);

    // make sure log.hflush() calls syncFs() to open a pipeline
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // lower the namenode & datanode heartbeat so the namenode
    // quickly detects datanode failures
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    // the namenode might still try to choose the recently-dead datanode
    // for a pipeline, so try to a new pipeline multiple times
    TEST_UTIL.getConfiguration().setInt("dfs.client.block.write.retries", 10);
  }

  private Configuration conf;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    TEST_UTIL.startMiniCluster(2);

    cluster = TEST_UTIL.getHBaseCluster();
    dfsCluster = TEST_UTIL.getDFSCluster();
    admin = TEST_UTIL.getHBaseAdmin();
    conf = TEST_UTIL.getConfiguration();
    fs = TEST_UTIL.getDFSCluster().getFileSystem();

    // disable region rebalancing (interferes with log watching)
    cluster.getMaster().balanceSwitch(false);
  }

  @After
  public void tearDown() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  /**
   * Tests that RegionServer aborts if we hit an error closing the WAL when
   * there are unsynced WAL edits.  See HBASE-4282.
   */
  @Test
  public void testRSAbortWithUnflushedEdits() throws Exception {
    LOG.info("Starting testRSAbortWithUnflushedEdits()");

    // When the hbase:meta table can be opened, the region servers are running
    new HTable(TEST_UTIL.getConfiguration(),
      TableName.META_TABLE_NAME).close();

    // Create the test table and open it
    TableName tableName = TableName.valueOf(this.getClass().getSimpleName());
    HTableDescriptor desc = new HTableDescriptor(tableName);
    desc.addFamily(new HColumnDescriptor(HConstants.CATALOG_FAMILY));

    admin.createTable(desc);
    Table table = new HTable(TEST_UTIL.getConfiguration(), desc.getTableName());
    try {

      HRegionServer server = TEST_UTIL.getRSForFirstRegionInTable(tableName);
      WALService log = server.getWAL();

      // don't run this test without append support (HDFS-200 & HDFS-142)
      assertTrue("Need append support for this test",
        FSUtils.isAppendSupported(TEST_UTIL.getConfiguration()));

      Put p = new Put(Bytes.toBytes("row2001"));
      p.add(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2001));
      table.put(p);

      log.sync();

      p = new Put(Bytes.toBytes("row2002"));
      p.add(HConstants.CATALOG_FAMILY, Bytes.toBytes("col"), Bytes.toBytes(2002));
      table.put(p);

      dfsCluster.restartDataNodes();
      LOG.info("Restarted datanodes");

      try {
        log.rollWriter(true);
      } catch (FailedLogCloseException flce) {
        // Expected exception.  We used to expect that there would be unsynced appends but this
        // not reliable now that sync plays a roll in wall rolling.  The above puts also now call
        // sync.
      } catch (Throwable t) {
        LOG.fatal("FAILED TEST: Got wrong exception", t);
      }
    } finally {
      table.close();
    }
  }

  /**
   * Tests the case where a RegionServer enters a GC pause,
   * comes back online after the master declared it dead and started to split.
   * Want log rolling after a master split to fail. See HBASE-2312.
   */
  @Test (timeout=300000)
  public void testLogRollAfterSplitStart() throws IOException {
    LOG.info("Verify wal roll after split starts will fail.");
    AbstractWAL log = null;
    String logName = "testLogRollAfterSplitStart";
    Path thisTestsDir = new Path(HBASEDIR, logName);

    try {
      // put some entries in an HLog
      TableName tableName =
          TableName.valueOf(this.getClass().getName());
      HRegionInfo regioninfo = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      log = (AbstractWAL)HLogFactory.createHLog(fs, HBASEDIR, logName, conf);
      final AtomicLong sequenceId = new AtomicLong(1);

      final int total = 20;
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        HTableDescriptor htd = new HTableDescriptor(tableName);
        htd.addFamily(new HColumnDescriptor("column"));
        log.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Send the data to HDFS datanodes and close the HDFS writer
      log.sync();
      ((FSHLog) log).replaceWriter(((FSHLog)log).getOldPath(), null, null, null);

      /* code taken from MasterFileSystem.getLogDirs(), which is called from MasterFileSystem.splitLog()
       * handles RS shutdowns (as observed by the splitting process)
       */
      // rename the directory so a rogue RS doesn't create more HLogs
      Path rsSplitDir = thisTestsDir.suffix(WAL.SPLITTING_EXT);
      if (!fs.rename(thisTestsDir, rsSplitDir)) {
        throw new IOException("Failed fs.rename for log split: " + thisTestsDir);
      }
      LOG.debug("Renamed region directory: " + rsSplitDir);

      LOG.debug("Processing the old log files.");
      HLogSplitter.split(HBASEDIR, rsSplitDir, OLDLOGDIR, fs, conf);

      LOG.debug("Trying to roll the HLog.");
      try {
        log.rollWriter();
        Assert.fail("rollWriter() did not throw any exception.");
      } catch (IOException ioe) {
        if (ioe.getCause() instanceof FileNotFoundException) {
          LOG.info("Got the expected exception: ", ioe.getCause());
        } else {
          Assert.fail("Unexpected exception: " + ioe);
        }
      }
    } finally {
      if (log != null) {
        log.close();
      }
      if (fs.exists(thisTestsDir)) {
        fs.delete(thisTestsDir, true);
      }
    }
  }
}
