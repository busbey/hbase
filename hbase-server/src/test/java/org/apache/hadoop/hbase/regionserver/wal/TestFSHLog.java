/**
 *
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableBoolean;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Provides FSHLog test cases. Since FSHLog is the default WAL implementation, we can use the
 * default configuration.
 */
@Category({RegionServerTests.class, LargeTests.class})
public class TestFSHLog extends TestHLog {

  @Test @Override
  public void testLogCleaning() throws Exception {
    LOG.info("testLogCleaning");
    final TableName tableName =
        TableName.valueOf("testLogCleaning");
    final TableName tableName2 =
        TableName.valueOf("testLogCleaning2");

    FSHLog log = (FSHLog)HLogFactory.createHLog(fs, hbaseDir,
        getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(tableName2,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);

      // Add a single edit and make sure that rolling won't remove the file
      // Before HBASE-3198 it used to delete it
      addEdits(log, hri, tableName, 1, sequenceId);
      log.rollWriter();
      assertEquals(1, log.getNumRolledLogFiles());

      // See if there's anything wrong with more than 1 edit
      addEdits(log, hri, tableName, 2, sequenceId);
      log.rollWriter();
      assertEquals(2, log.getNumRolledLogFiles());

      // Now mix edits from 2 regions, still no flushing
      addEdits(log, hri, tableName, 1, sequenceId);
      addEdits(log, hri2, tableName2, 1, sequenceId);
      addEdits(log, hri, tableName, 1, sequenceId);
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.rollWriter();
      assertEquals(3, log.getNumRolledLogFiles());

      // Flush the first region, we expect to see the first two files getting
      // archived. We need to append something or writer won't be rolled.
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.startCacheFlush(hri.getEncodedNameAsBytes());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(2, log.getNumRolledLogFiles());

      // Flush the second region, which removes all the remaining output files
      // since the oldest was completely flushed and the two others only contain
      // flush information
      addEdits(log, hri2, tableName2, 1, sequenceId);
      log.startCacheFlush(hri2.getEncodedNameAsBytes());
      log.completeCacheFlush(hri2.getEncodedNameAsBytes());
      log.rollWriter();
      assertEquals(0, log.getNumRolledLogFiles());
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  /**
   * tests the log comparator. Ensure that we are not mixing meta logs with non-meta logs (throws
   * exception if we do). Comparison is based on the timestamp present in the wal name.
   * @throws Exception
   */
  @Test @Override
  public void testHLogComparator() throws Exception {
    FSHLog hlog1 = null;
    FSHLog hlogMeta = null;
    try {
      hlog1 = (FSHLog) HLogFactory.createHLog(fs, FSUtils.getRootDir(conf), dir.toString(), conf);
      LOG.debug("Log obtained is: " + hlog1);
      Comparator<Path> comp = hlog1.LOG_NAME_COMPARATOR;
      Path p1 = hlog1.computeFilename(11);
      Path p2 = hlog1.computeFilename(12);
      // comparing with itself returns 0
      assertTrue(comp.compare(p1, p1) == 0);
      // comparing with different filenum.
      assertTrue(comp.compare(p1, p2) < 0);
      hlogMeta = (FSHLog) HLogFactory.createMetaHLog(fs, FSUtils.getRootDir(conf), dir.toString(),
        conf, null, null);
      Comparator<Path> compMeta = hlogMeta.LOG_NAME_COMPARATOR;

      Path p1WithMeta = hlogMeta.computeFilename(11);
      Path p2WithMeta = hlogMeta.computeFilename(12);
      assertTrue(compMeta.compare(p1WithMeta, p1WithMeta) == 0);
      assertTrue(compMeta.compare(p1WithMeta, p2WithMeta) < 0);
      // mixing meta and non-meta logs gives error
      boolean ex = false;
      try {
        comp.compare(p1WithMeta, p2);
      } catch (Exception e) {
        ex = true;
      }
      assertTrue("Comparator doesn't complain while checking meta log files", ex);
      boolean exMeta = false;
      try {
        compMeta.compare(p1WithMeta, p2);
      } catch (Exception e) {
        exMeta = true;
      }
      assertTrue("Meta comparator doesn't complain while checking log files", exMeta);
    } finally {
      if (hlog1 != null) hlog1.close();
      if (hlogMeta != null) hlogMeta.close();
    }
  }

  /**
   * Tests wal archiving by adding data, doing flushing/rolling and checking we archive old logs
   * and also don't archive "live logs" (that is, a log with un-flushed entries).
   * <p>
   * This is what it does:
   * It creates two regions, and does a series of inserts along with log rolling.
   * Whenever a WAL is rolled, HLogBase checks previous wals for archiving. A wal is eligible for
   * archiving if for all the regions which have entries in that wal file, have flushed - past
   * their maximum sequence id in that wal file.
   * <p>
   * @throws IOException
   */
  @Test @Override
  public void testWALArchiving() throws IOException {
    LOG.debug("testWALArchiving");
    TableName table1 = TableName.valueOf("t1");
    TableName table2 = TableName.valueOf("t2");
    FSHLog hlog = (FSHLog) HLogFactory.createHLog(fs, FSUtils.getRootDir(conf), dir.toString(),
      conf);
    try {
      assertEquals(0, hlog.getNumRolledLogFiles());
      HRegionInfo hri1 = new HRegionInfo(table1, HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      HRegionInfo hri2 = new HRegionInfo(table2, HConstants.EMPTY_START_ROW,
          HConstants.EMPTY_END_ROW);
      // ensure that we don't split the regions.
      hri1.setSplit(false);
      hri2.setSplit(false);
      // variables to mock region sequenceIds.
      final AtomicLong sequenceId1 = new AtomicLong(1);
      final AtomicLong sequenceId2 = new AtomicLong(1);
      // start with the testing logic: insert a waledit, and roll writer
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      hlog.rollWriter();
      // assert that the wal is rolled
      assertEquals(1, hlog.getNumRolledLogFiles());
      // add edits in the second wal file, and roll writer.
      addEdits(hlog, hri1, table1, 1, sequenceId1);
      hlog.rollWriter();
      // assert that the wal is rolled
      assertEquals(2, hlog.getNumRolledLogFiles());
      // add a waledit to table1, and flush the region.
      addEdits(hlog, hri1, table1, 3, sequenceId1);
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      // roll log; all old logs should be archived.
      hlog.rollWriter();
      assertEquals(0, hlog.getNumRolledLogFiles());
      // add an edit to table2, and roll writer
      addEdits(hlog, hri2, table2, 1, sequenceId2);
      hlog.rollWriter();
      assertEquals(1, hlog.getNumRolledLogFiles());
      // add edits for table1, and roll writer
      addEdits(hlog, hri1, table1, 2, sequenceId1);
      hlog.rollWriter();
      assertEquals(2, hlog.getNumRolledLogFiles());
      // add edits for table2, and flush hri1.
      addEdits(hlog, hri2, table2, 2, sequenceId2);
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      // the log : region-sequenceId map is
      // log1: region2 (unflushed)
      // log2: region1 (flushed)
      // log3: region2 (unflushed)
      // roll the writer; log2 should be archived.
      hlog.rollWriter();
      assertEquals(2, hlog.getNumRolledLogFiles());
      // flush region2, and all logs should be archived.
      addEdits(hlog, hri2, table2, 2, sequenceId2);
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter();
      assertEquals(0, hlog.getNumRolledLogFiles());
    } finally {
      if (hlog != null) hlog.close();
    }
  }

  /**
   * On rolling a wal after reaching the threshold, {@link WALService#rollWriter()} returns the
   * list of regions which should be flushed in order to archive the oldest wal file.
   * <p>
   * This method tests this behavior by inserting edits and rolling the wal enough times to reach
   * the max number of logs threshold. It checks whether we get the "right regions" for flush on
   * rolling the wal.
   * @throws Exception
   */
  @Test @Override
  public void testFindMemStoresEligibleForFlush() throws Exception {
    LOG.debug("testFindMemStoresEligibleForFlush");
    Configuration conf1 = HBaseConfiguration.create(conf);
    conf1.setInt("hbase.regionserver.maxlogs", 1);
    FSHLog hlog = (FSHLog) HLogFactory.createHLog(fs, FSUtils.getRootDir(conf1), dir.toString(),
      conf1);
    TableName t1 = TableName.valueOf("t1");
    TableName t2 = TableName.valueOf("t2");
    HRegionInfo hri1 = new HRegionInfo(t1, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    HRegionInfo hri2 = new HRegionInfo(t2, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // variables to mock region sequenceIds
    final AtomicLong sequenceId1 = new AtomicLong(1);
    final AtomicLong sequenceId2 = new AtomicLong(1);
    // add edits and roll the wal
    try {
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // add some more edits and roll the wal. This would reach the log number threshold
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // with above rollWriter call, the max logs limit is reached.
      assertTrue(hlog.getNumRolledLogFiles() == 2);

      // get the regions to flush; since there is only one region in the oldest wal, it should
      // return only one region.
      byte[][] regionsToFlush = hlog.findRegionsToForceFlush();
      assertEquals(1, regionsToFlush.length);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // insert edits in second region
      addEdits(hlog, hri2, t2, 2, sequenceId2);
      // get the regions to flush, it should still read region1.
      regionsToFlush = hlog.findRegionsToForceFlush();
      assertEquals(regionsToFlush.length, 1);
      assertEquals(hri1.getEncodedNameAsBytes(), regionsToFlush[0]);
      // flush region 1, and roll the wal file. Only last wal which has entries for region1 should
      // remain.
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      hlog.rollWriter();
      // only one wal should remain now (that is for the second region).
      assertEquals(1, hlog.getNumRolledLogFiles());
      // flush the second region
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter(true);
      // no wal should remain now.
      assertEquals(0, hlog.getNumRolledLogFiles());
      // add edits both to region 1 and region 2, and roll.
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      addEdits(hlog, hri2, t2, 2, sequenceId2);
      hlog.rollWriter();
      // add edits and roll the writer, to reach the max logs limit.
      assertEquals(1, hlog.getNumRolledLogFiles());
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      hlog.rollWriter();
      // it should return two regions to flush, as the oldest wal file has entries
      // for both regions.
      regionsToFlush = hlog.findRegionsToForceFlush();
      assertEquals(2, regionsToFlush.length);
      // flush both regions
      flushRegion(hlog, hri1.getEncodedNameAsBytes());
      flushRegion(hlog, hri2.getEncodedNameAsBytes());
      hlog.rollWriter(true);
      assertEquals(0, hlog.getNumRolledLogFiles());
      // Add an edit to region1, and roll the wal.
      addEdits(hlog, hri1, t1, 2, sequenceId1);
      // tests partial flush: roll on a partial flush, and ensure that wal is not archived.
      hlog.startCacheFlush(hri1.getEncodedNameAsBytes());
      hlog.rollWriter();
      hlog.completeCacheFlush(hri1.getEncodedNameAsBytes());
      assertEquals(1, hlog.getNumRolledLogFiles());
    } finally {
      if (hlog != null) hlog.close();
    }
  }

  /**
   * Simulates HLog append ops for a region and tests
   * {@link FSHLog#areAllRegionsFlushed(Map, Map, Map)} API.
   * It compares the region sequenceIds with oldestFlushing and oldestUnFlushed entries.
   * If a region's entries are larger than min of (oldestFlushing, oldestUnFlushed), then the
   * region should be flushed before archiving this WAL.
  */
  @Test
  public void testAllRegionsFlushed() {
    LOG.debug("testAllRegionsFlushed");
    Map<byte[], Long> oldestFlushingSeqNo = new HashMap<byte[], Long>();
    Map<byte[], Long> oldestUnFlushedSeqNo = new HashMap<byte[], Long>();
    Map<byte[], Long> seqNo = new HashMap<byte[], Long>();
    // create a table
    TableName t1 = TableName.valueOf("t1");
    // create a region
    HRegionInfo hri1 = new HRegionInfo(t1, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
    // variables to mock region sequenceIds
    final AtomicLong sequenceId1 = new AtomicLong(1);
    // test empty map
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // add entries in the region
    seqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.incrementAndGet());
    oldestUnFlushedSeqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.get());
    // should say region1 is not flushed.
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // test with entries in oldestFlushing map.
    oldestUnFlushedSeqNo.clear();
    oldestFlushingSeqNo.put(hri1.getEncodedNameAsBytes(), sequenceId1.get());
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // simulate region flush, i.e., clear oldestFlushing and oldestUnflushed maps
    oldestFlushingSeqNo.clear();
    oldestUnFlushedSeqNo.clear();
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
    // insert some large values for region1
    oldestUnFlushedSeqNo.put(hri1.getEncodedNameAsBytes(), 1000l);
    seqNo.put(hri1.getEncodedNameAsBytes(), 1500l);
    assertFalse(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));

    // tests when oldestUnFlushed/oldestFlushing contains larger value.
    // It means region is flushed.
    oldestFlushingSeqNo.put(hri1.getEncodedNameAsBytes(), 1200l);
    oldestUnFlushedSeqNo.clear();
    seqNo.put(hri1.getEncodedNameAsBytes(), 1199l);
    assertTrue(FSHLog.areAllRegionsFlushed(seqNo, oldestFlushingSeqNo, oldestUnFlushedSeqNo));
  }

  @Test(expected=IOException.class)
  public void testFailedToCreateHLogIfParentRenamed() throws IOException {
    FSHLog log = (FSHLog)HLogFactory.createHLog(
      fs, hbaseDir, "testFailedToCreateHLogIfParentRenamed", conf);
    long filenum = System.currentTimeMillis();
    Path path = log.computeFilename(filenum);
    HLogFactory.createWALWriter(fs, path, conf);
    Path parent = path.getParent();
    path = log.computeFilename(filenum + 1);
    Path newPath = new Path(parent.getParent(), parent.getName() + "-splitting");
    fs.rename(parent, newPath);
    HLogFactory.createWALWriter(fs, path, conf);
    fail("It should fail to create the new WAL");
  }

  /**
   * Test flush for sure has a sequence id that is beyond the last edit appended.  We do this
   * by slowing appends in the background ring buffer thread while in foreground we call
   * flush.  The addition of the sync over HRegion in flush should fix an issue where flush was
   * returning before all of its appends had made it out to the WAL (HBASE-11109).
   * @throws IOException
   * @see HBASE-11109
   */
  @Test
  public void testFlushSequenceIdIsGreaterThanAllEditsInHFile() throws IOException {
    String testName = "testFlushSequenceIdIsGreaterThanAllEditsInHFile";
    final TableName tableName = TableName.valueOf(testName);
    final HRegionInfo hri = new HRegionInfo(tableName);
    final byte[] rowName = tableName.getName();
    final HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("f"));
    HRegion r = HRegion.createHRegion(hri, TEST_UTIL.getDefaultRootDirPath(),
      TEST_UTIL.getConfiguration(), htd);
    HRegion.closeHRegion(r);
    final int countPerFamily = 10;
    final MutableBoolean goslow = new MutableBoolean(false);
    // Bypass factory so I can subclass and doctor a method.
    FSHLog wal = new FSHLog(FileSystem.get(conf), TEST_UTIL.getDefaultRootDirPath(),
        testName, conf) {
      @Override
      void atHeadOfRingBufferEventHandlerAppend() {
        if (goslow.isTrue()) {
          Threads.sleep(100);
          LOG.debug("Sleeping before appending 100ms");
        }
        super.atHeadOfRingBufferEventHandlerAppend();
      }
    };
    HRegion region = HRegion.openHRegion(TEST_UTIL.getConfiguration(),
      TEST_UTIL.getTestFileSystem(), TEST_UTIL.getDefaultRootDirPath(), hri, htd, wal);
    EnvironmentEdge ee = EnvironmentEdgeManager.getDelegate();
    try {
      List<Put> puts = null;
      for (HColumnDescriptor hcd: htd.getFamilies()) {
        puts =
          TestWALReplay.addRegionEdits(rowName, hcd.getName(), countPerFamily, ee, region, "x");
      }

      // Now assert edits made it in.
      final Get g = new Get(rowName);
      Result result = region.get(g);
      assertEquals(countPerFamily * htd.getFamilies().size(), result.size());

      // Construct a WALEdit and add it a few times to the WAL.
      WALEdit edits = new WALEdit();
      for (Put p: puts) {
        CellScanner cs = p.cellScanner();
        while (cs.advance()) {
          edits.add(cs.current());
        }
      }
      // Add any old cluster id.
      List<UUID> clusterIds = new ArrayList<UUID>();
      clusterIds.add(UUID.randomUUID());
      // Now make appends run slow.
      goslow.setValue(true);
      for (int i = 0; i < countPerFamily; i++) {
        wal.appendNoSync(region.getRegionInfo(), tableName, edits,
          clusterIds, System.currentTimeMillis(), htd, region.getSequenceId(), true, -1, -1);
      }
      region.flushcache();
      // FlushResult.flushSequenceId is not visible here so go get the current sequence id.
      long currentSequenceId = region.getSequenceId().get();
      // Now release the appends
      goslow.setValue(false);
      synchronized (goslow) {
        goslow.notifyAll();
      }
      assertTrue(currentSequenceId >= region.getSequenceId().get());
    } finally {
      region.close(true);
      wal.close();
    }
  }

  /**
   * Write to a log file with three concurrent threads and verifying all data is written.
   * @throws Exception
   */
  @Test
  public void testConcurrentWrites() throws Exception {
    // Run the HPE tool with three threads writing 3000 edits each concurrently.
    // When done, verify that all edits were written.
    int errCode = HLogPerformanceEvaluation.
      innerMain(new Configuration(TEST_UTIL.getConfiguration()),
        new String [] {"-threads", "3", "-verify", "-noclosefs", "-iterations", "3000"});
    assertEquals(0, errCode);
  }
}
