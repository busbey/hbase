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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.lang.reflect.Method;
import java.net.BindException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.impl.Log4JLogger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.testclassification.LargeTests;
import org.apache.hadoop.hbase.testclassification.RegionServerTests;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.coprocessor.CoprocessorHost;
import org.apache.hadoop.hbase.coprocessor.SampleRegionWALObserver;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.EnvironmentEdge;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.FSConstants;
import org.apache.hadoop.hdfs.server.datanode.DataNode;
import org.apache.hadoop.hdfs.server.namenode.LeaseManager;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * The base class to test WAL implementations. It has concrete test methods for basic WAL
 * functionalities (such as append/sync data) which all WAL implementations should provide.
 * It also provides abstract methods which are specific to a implementation (such as number of WAL
 * files rolled, etc). All concrete WAL implementation should provide a test class with TestHLog
 * as the base class. See {@link TestFSHLog} for example. 
 */
@Category({RegionServerTests.class, LargeTests.class})
@SuppressWarnings("deprecation")
public abstract class TestHLog  {
  protected static final Log LOG = LogFactory.getLog(TestHLog.class);
  {
    // Uncomment the following lines if more verbosity is needed for
    // debugging (see HBASE-12285 for details).
    //((Log4JLogger)DataNode.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LeaseManager.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)LogFactory.getLog("org.apache.hadoop.hdfs.server.namenode.FSNamesystem"))
    //    .getLogger().setLevel(Level.ALL);
    //((Log4JLogger)DFSClient.LOG).getLogger().setLevel(Level.ALL);
    //((Log4JLogger)WALService.LOG).getLogger().setLevel(Level.ALL);
  }

  protected static Configuration conf;
  protected static FileSystem fs;
  protected static Path dir;
  private static MiniDFSCluster cluster;
  protected final static HBaseTestingUtility TEST_UTIL = new HBaseTestingUtility();
  protected static Path hbaseDir;
  private static Path oldLogDir;

  @Before
  public void setUp() throws Exception {
    FileStatus[] entries = fs.listStatus(new Path("/"));
    for (FileStatus dir : entries) {
      fs.delete(dir.getPath(), true);
    }
  }

  @After
  public void tearDown() throws Exception {
  }

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    // Make block sizes small.
    TEST_UTIL.getConfiguration().setInt("dfs.blocksize", 1024 * 1024);
    // needed for testAppendClose()
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.broken.append", true);
    TEST_UTIL.getConfiguration().setBoolean("dfs.support.append", true);
    // quicker heartbeat interval for faster DN death notification
    TEST_UTIL.getConfiguration().setInt("dfs.namenode.heartbeat.recheck-interval", 5000);
    TEST_UTIL.getConfiguration().setInt("dfs.heartbeat.interval", 1);
    TEST_UTIL.getConfiguration().setInt("dfs.client.socket-timeout", 5000);

    // faster failover with cluster.shutdown();fs.close() idiom
    TEST_UTIL.getConfiguration()
        .setInt("hbase.ipc.client.connect.max.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
        "dfs.client.block.recovery.retries", 1);
    TEST_UTIL.getConfiguration().setInt(
      "hbase.ipc.client.connection.maxidletime", 500);
    TEST_UTIL.getConfiguration().set(CoprocessorHost.WAL_COPROCESSOR_CONF_KEY,
        SampleRegionWALObserver.class.getName());
    TEST_UTIL.startMiniDFSCluster(3);

    conf = TEST_UTIL.getConfiguration();
    cluster = TEST_UTIL.getDFSCluster();
    fs = cluster.getFileSystem();

    hbaseDir = TEST_UTIL.createRootDir();
    oldLogDir = new Path(hbaseDir, HConstants.HREGION_OLDLOGDIR_NAME);
    dir = new Path(hbaseDir, getName());
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    TEST_UTIL.shutdownMiniCluster();
  }

  static String getName() {
    // TODO Auto-generated method stub
    return "TestHLog";
  }

  /**
   * Just write multiple logs then split.  Before fix for HADOOP-2283, this
   * would fail.
   * @throws IOException
   */
  @Test
  public void testSplit() throws IOException {
    final TableName tableName =
        TableName.valueOf(getName());
    final byte [] rowName = tableName.getName();
    Path logdir = new Path(hbaseDir, HConstants.HREGION_LOGDIR_NAME);
    WALService log = HLogFactory.createHLog(fs, hbaseDir, HConstants.HREGION_LOGDIR_NAME, conf);
    final int howmany = 3;
    HRegionInfo[] infos = new HRegionInfo[3];
    Path tabledir = FSUtils.getTableDir(hbaseDir, tableName);
    fs.mkdirs(tabledir);
    for(int i = 0; i < howmany; i++) {
      infos[i] = new HRegionInfo(tableName,
                Bytes.toBytes("" + i), Bytes.toBytes("" + (i+1)), false);
      fs.mkdirs(new Path(tabledir, infos[i].getEncodedName()));
      LOG.info("allo " + new Path(tabledir, infos[i].getEncodedName()).toString());
    }
    HTableDescriptor htd = new HTableDescriptor(tableName);
    htd.addFamily(new HColumnDescriptor("column"));

    // Add edits for three regions.
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      for (int ii = 0; ii < howmany; ii++) {
        for (int i = 0; i < howmany; i++) {

          for (int j = 0; j < howmany; j++) {
            WALEdit edit = new WALEdit();
            byte [] family = Bytes.toBytes("column");
            byte [] qualifier = Bytes.toBytes(Integer.toString(j));
            byte [] column = Bytes.toBytes("column:" + Integer.toString(j));
            edit.add(new KeyValue(rowName, family, qualifier,
                System.currentTimeMillis(), column));
            LOG.info("Region " + i + ": " + edit);
            log.append(infos[i], tableName, edit,
              System.currentTimeMillis(), htd, sequenceId);
          }
        }
        log.rollWriter();
      }
      log.close();
      List<Path> splits = HLogSplitter.split(hbaseDir, logdir, oldLogDir, fs, conf);
      verifySplits(splits, howmany);
      log = null;
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
    }
  }

  /**
   * Test new HDFS-265 sync.
   * @throws Exception
   */
  @Test
  public void Broken_testSync() throws Exception {
    TableName tableName =
        TableName.valueOf(getName());
    // First verify that using streams all works.
    Path p = new Path(dir, getName() + ".fsdos");
    FSDataOutputStream out = fs.create(p);
    out.write(tableName.getName());
    Method syncMethod = null;
    try {
      syncMethod = out.getClass().getMethod("hflush", new Class<?> []{});
    } catch (NoSuchMethodException e) {
      try {
        syncMethod = out.getClass().getMethod("sync", new Class<?> []{});
      } catch (NoSuchMethodException ex) {
        fail("This version of Hadoop supports neither Syncable.sync() " +
            "nor Syncable.hflush().");
      }
    }
    syncMethod.invoke(out, new Object[]{});
    FSDataInputStream in = fs.open(p);
    assertTrue(in.available() > 0);
    byte [] buffer = new byte [1024];
    int read = in.read(buffer);
    assertEquals(tableName.getName().length, read);
    out.close();
    in.close();

    WALService wal = HLogFactory.createHLog(fs, dir, "hlogdir", conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;
    WAL.Reader reader = null;

    try {
      HRegionInfo info = new HRegionInfo(tableName,
                  null,null, false);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor(tableName.getName()));

      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Now call sync and try reading.  Opening a Reader before you sync just
      // gives you EOFE.
      wal.sync();
      // Open a Reader.
      Path walPath = ((AbstractWAL) wal).getCurrentFileName();
      reader = HLogFactory.createReader(fs, walPath, conf);
      int count = 0;
      WAL.Entry entry = new WAL.Entry();
      while ((entry = reader.next(entry)) != null) count++;
      assertEquals(total, count);
      reader.close();
      // Add test that checks to see that an open of a Reader works on a file
      // that has had a sync done on it.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertTrue(count >= total);
      reader.close();
      // If I sync, should see double the edits.
      wal.sync();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 2, count);
      // Now do a test that ensures stuff works when we go over block boundary,
      // especially that we return good length on file.
      final byte [] value = new byte[1025 * 1024];  // Make a 1M value.
      for (int i = 0; i < total; i++) {
        WALEdit kvs = new WALEdit();
        kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), value));
        wal.append(info, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
      }
      // Now I should have written out lots of blocks.  Sync then read.
      wal.sync();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
      // Close it and ensure that closed, Reader gets right length also.
      wal.close();
      reader = HLogFactory.createReader(fs, walPath, conf);
      count = 0;
      while((entry = reader.next(entry)) != null) count++;
      assertEquals(total * 3, count);
      reader.close();
    } finally {
      if (wal != null) wal.closeAndDelete();
      if (reader != null) reader.close();
    }
  }

  private void verifySplits(List<Path> splits, final int howmany)
  throws IOException {
    assertEquals(howmany * howmany, splits.size());
    for (int i = 0; i < splits.size(); i++) {
      LOG.info("Verifying=" + splits.get(i));
      WAL.Reader reader = HLogFactory.createReader(fs, splits.get(i), conf);
      try {
        int count = 0;
        String previousRegion = null;
        long seqno = -1;
        WAL.Entry entry = new WAL.Entry();
        while((entry = reader.next(entry)) != null) {
          HLogKey key = entry.getKey();
          String region = Bytes.toString(key.getEncodedRegionName());
          // Assert that all edits are for same region.
          if (previousRegion != null) {
            assertEquals(previousRegion, region);
          }
          LOG.info("oldseqno=" + seqno + ", newseqno=" + key.getLogSeqNum());
          assertTrue(seqno < key.getLogSeqNum());
          seqno = key.getLogSeqNum();
          previousRegion = region;
          count++;
        }
        assertEquals(howmany, count);
      } finally {
        reader.close();
      }
    }
  }

  /*
   * We pass different values to recoverFileLease() so that different code paths are covered
   *
   * For this test to pass, requires:
   * 1. HDFS-200 (append support)
   * 2. HDFS-988 (SafeMode should freeze file operations
   *              [FSNamesystem.nextGenerationStampForBlock])
   * 3. HDFS-142 (on restart, maintain pendingCreates)
   */
  @Test (timeout=300000)
  public void testAppendClose() throws Exception {
    TableName tableName =
        TableName.valueOf(getName());
    HRegionInfo regioninfo = new HRegionInfo(tableName,
             HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, false);

    WALService wal = HLogFactory.createHLog(fs, dir, "hlogdir",
        "hlogdir_archive", conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    final int total = 20;

    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor(tableName.getName()));

    for (int i = 0; i < total; i++) {
      WALEdit kvs = new WALEdit();
      kvs.add(new KeyValue(Bytes.toBytes(i), tableName.getName(), tableName.getName()));
      wal.append(regioninfo, tableName, kvs, System.currentTimeMillis(), htd, sequenceId);
    }
    // Now call sync to send the data to HDFS datanodes
    wal.sync();
     int namenodePort = cluster.getNameNodePort();
    final Path walPath = ((AbstractWAL) wal).getCurrentFileName();


    // Stop the cluster.  (ensure restart since we're sharing MiniDFSCluster)
    try {
      DistributedFileSystem dfs = (DistributedFileSystem) cluster.getFileSystem();
      dfs.setSafeMode(FSConstants.SafeModeAction.SAFEMODE_ENTER);
      TEST_UTIL.shutdownMiniDFSCluster();
      try {
        // wal.writer.close() will throw an exception,
        // but still call this since it closes the LogSyncer thread first
        wal.close();
      } catch (IOException e) {
        LOG.info(e);
      }
      fs.close(); // closing FS last so DFSOutputStream can't call close
      LOG.info("STOPPED first instance of the cluster");
    } finally {
      // Restart the cluster
      while (cluster.isClusterUp()){
        LOG.error("Waiting for cluster to go down");
        Thread.sleep(1000);
      }
      assertFalse(cluster.isClusterUp());
      cluster = null;
      for (int i = 0; i < 100; i++) {
        try {
          cluster = TEST_UTIL.startMiniDFSClusterForTestHLog(namenodePort);
          break;
        } catch (BindException e) {
          LOG.info("Sleeping.  BindException bringing up new cluster");
          Threads.sleep(1000);
        }
      }
      cluster.waitActive();
      fs = cluster.getFileSystem();
      LOG.info("STARTED second instance.");
    }

    // set the lease period to be 1 second so that the
    // namenode triggers lease recovery upon append request
    Method setLeasePeriod = cluster.getClass()
      .getDeclaredMethod("setLeasePeriod", new Class[]{Long.TYPE, Long.TYPE});
    setLeasePeriod.setAccessible(true);
    setLeasePeriod.invoke(cluster, 1000L, 1000L);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      LOG.info(e);
    }

    // Now try recovering the log, like the HMaster would do
    final FileSystem recoveredFs = fs;
    final Configuration rlConf = conf;

    class RecoverLogThread extends Thread {
      public Exception exception = null;
      public void run() {
          try {
            FSUtils.getInstance(fs, rlConf)
              .recoverFileLease(recoveredFs, walPath, rlConf, null);
          } catch (IOException e) {
            exception = e;
          }
      }
    }

    RecoverLogThread t = new RecoverLogThread();
    t.start();
    // Timeout after 60 sec. Without correct patches, would be an infinite loop
    t.join(60 * 1000);
    if(t.isAlive()) {
      t.interrupt();
      throw new Exception("Timed out waiting for HLog.recoverLog()");
    }

    if (t.exception != null)
      throw t.exception;

    // Make sure you can read all the content
    WAL.Reader reader = HLogFactory.createReader(fs, walPath, conf);
    int count = 0;
    WAL.Entry entry = new WAL.Entry();
    while (reader.next(entry) != null) {
      count++;
      assertTrue("Should be one KeyValue per WALEdit",
                  entry.getEdit().getCells().size() == 1);
    }
    assertEquals(total, count);
    reader.close();

    // Reset the lease period
    setLeasePeriod.invoke(cluster, new Object[]{new Long(60000), new Long(3600000)});
  }

  /**
   * Tests that we can write out an edit, close, and then read it back in again.
   * @throws IOException
   */
  @Test
  public void testEditAdd() throws IOException {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    WAL.Reader reader = null;
    WALService log = null;
    try {
      log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
      final AtomicLong sequenceId = new AtomicLong(1);

      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo info = new HRegionInfo(tableName,
        row,Bytes.toBytes(Bytes.toString(row) + "1"), false);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));

      log.append(info, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      log.startCacheFlush(info.getEncodedNameAsBytes());
      log.completeCacheFlush(info.getEncodedNameAsBytes());
      log.close();
      Path filename = ((AbstractWAL) log).getCurrentFileName();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLogFactory.createReader(fs, filename, conf);
      // Above we added all columns on a single row so we only read one
      // entry in the below... thats why we have '1'.
      for (int i = 0; i < 1; i++) {
        WAL.Entry entry = reader.next(null);
        if (entry == null) break;
        HLogKey key = entry.getKey();
        WALEdit val = entry.getEdit();
        assertTrue(Bytes.equals(info.getEncodedNameAsBytes(), key.getEncodedRegionName()));
        assertTrue(tableName.equals(key.getTablename()));
        Cell cell = val.getCells().get(0);
        assertTrue(Bytes.equals(row, cell.getRow()));
        assertEquals((byte)(i + '0'), cell.getValue()[0]);
        System.out.println(key + " " + val);
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * @throws IOException
   */
  @Test
  public void testAppend() throws IOException {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    WAL.Reader reader = null;
    WALService log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      // Write columns named 1, 2, 3, etc. and then values of single byte
      // 1, 2, 3...
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      for (int i = 0; i < COL_COUNT; i++) {
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(i)),
          timestamp, new byte[] { (byte)(i + '0') }));
      }
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));
      log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      log.startCacheFlush(hri.getEncodedNameAsBytes());
      log.completeCacheFlush(hri.getEncodedNameAsBytes());
      log.close();
      Path filename = ((AbstractWAL) log).getCurrentFileName();
      log = null;
      // Now open a reader on the log and assert append worked.
      reader = HLogFactory.createReader(fs, filename, conf);
      WAL.Entry entry = reader.next();
      assertEquals(COL_COUNT, entry.getEdit().size());
      int idx = 0;
      for (Cell val : entry.getEdit().getCells()) {
        assertTrue(Bytes.equals(hri.getEncodedNameAsBytes(),
          entry.getKey().getEncodedRegionName()));
        assertTrue(tableName.equals(entry.getKey().getTablename()));
        assertTrue(Bytes.equals(row, val.getRow()));
        assertEquals((byte)(idx + '0'), val.getValue()[0]);
        System.out.println(entry.getKey() + " " + val);
        idx++;
      }
    } finally {
      if (log != null) {
        log.closeAndDelete();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Test that we can visit entries before they are appended
   * @throws Exception
   */
  @Test
  public void testVisitors() throws Exception {
    final int COL_COUNT = 10;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte [] row = Bytes.toBytes("row");
    WALService log = HLogFactory.createHLog(fs, hbaseDir, getName(), conf);
    final AtomicLong sequenceId = new AtomicLong(1);
    try {
      DumbWALActionsListener visitor = new DumbWALActionsListener();
      log.registerWALActionsListener(visitor);
      long timestamp = System.currentTimeMillis();
      HTableDescriptor htd = new HTableDescriptor();
      htd.addFamily(new HColumnDescriptor("column"));

      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      for (int i = 0; i < COL_COUNT; i++) {
        WALEdit cols = new WALEdit();
        cols.add(new KeyValue(row, Bytes.toBytes("column"),
            Bytes.toBytes(Integer.toString(i)),
            timestamp, new byte[]{(byte) (i + '0')}));
        log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      }
      assertEquals(COL_COUNT, visitor.increments);
      log.unregisterWALActionsListener(visitor);
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, Bytes.toBytes("column"),
          Bytes.toBytes(Integer.toString(11)),
          timestamp, new byte[]{(byte) (11 + '0')}));
      log.append(hri, tableName, cols, System.currentTimeMillis(), htd, sequenceId);
      assertEquals(COL_COUNT, visitor.increments);
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  @Test
  public void testGetServerNameFromHLogDirectoryName() throws IOException {
    ServerName sn = ServerName.valueOf("hn", 450, 1398);
    String hl = FSUtils.getRootDir(conf) + "/" + HLogUtil.getHLogDirectoryName(sn.toString());

    // Must not throw exception
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, null));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf,
        FSUtils.getRootDir(conf).toUri().toString()));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, ""));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, "                  "));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, hl));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, hl + "qdf"));
    Assert.assertNull(HLogUtil.getServerNameFromHLogDirectoryName(conf, "sfqf" + hl + "qdf"));

    final String wals = "/WALs/";
    ServerName parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
      "/localhost%2C32984%2C1343316388997.1343316390417");
    Assert.assertEquals("standard",  sn, parsed);

    parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf, hl + "/qdf");
    Assert.assertEquals("subdir", sn, parsed);

    parsed = HLogUtil.getServerNameFromHLogDirectoryName(conf,
      FSUtils.getRootDir(conf).toUri().toString() + wals + sn +
      "-splitting/localhost%3A57020.1340474893931");
    Assert.assertEquals("split", sn, parsed);
  }

  /**
   * A loaded WAL coprocessor won't break existing HLog test cases.
   */
  @Test
  public void testWALCoprocessorLoaded() throws Exception {
    // test to see whether the coprocessor is loaded or not.
    WALService log = HLogFactory.createHLog(fs, hbaseDir,
        getName(), conf);
    try {
      WALCoprocessorHost host = log.getCoprocessorHost();
      Coprocessor c = host.findCoprocessor(SampleRegionWALObserver.class.getName());
      assertNotNull(c);
    } finally {
      if (log != null) log.closeAndDelete();
    }
  }

  protected void addEdits(WALService log, HRegionInfo hri, TableName tableName,
                        int times, AtomicLong sequenceId) throws IOException {
    HTableDescriptor htd = new HTableDescriptor();
    htd.addFamily(new HColumnDescriptor("row"));

    final byte [] row = Bytes.toBytes("row");
    for (int i = 0; i < times; i++) {
      long timestamp = System.currentTimeMillis();
      WALEdit cols = new WALEdit();
      cols.add(new KeyValue(row, row, row, timestamp, row));
      log.append(hri, tableName, cols, timestamp, htd, sequenceId);
    }
  }


  /**
   * @throws IOException
   */
  @Test
  public void testReadLegacyLog() throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "temphlog");
    SequenceFileLogWriter sflw = null;
    WAL.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pre-PB format.
      sflw = new SequenceFileLogWriter();
      sflw.init(fs, path, conf, false);
      for (int i = 0; i < recordCount; ++i) {
        HLogKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        sflw.append(new WAL.Entry(key, edit));
      }
      sflw.sync();
      sflw.close();

      // Now read the log using standard means.
      reader = HLogFactory.createReader(fs, path, conf);
      assertTrue(reader instanceof SequenceFileLogReader);
      for (int i = 0; i < recordCount; ++i) {
        WAL.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      WAL.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (sflw != null) {
        sflw.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  /**
   * Reads the WAL with and without WALTrailer.
   * @throws IOException
   */
  @Test
  public void testWALTrailer() throws IOException {
    // read With trailer.
    doRead(true);
    // read without trailer
    doRead(false);
  }

  /**
   * Appends entries in the WAL and reads it.
   * @param withTrailer If 'withTrailer' is true, it calls a close on the WALwriter before reading
   *          so that a trailer is appended to the WAL. Otherwise, it starts reading after the sync
   *          call. This means that reader is not aware of the trailer. In this scenario, if the
   *          reader tries to read the trailer in its next() call, it returns false from
   *          ProtoBufLogReader.
   * @throws IOException
   */
  private void doRead(boolean withTrailer) throws IOException {
    final int columnCount = 5;
    final int recordCount = 5;
    final TableName tableName =
        TableName.valueOf("tablename");
    final byte[] row = Bytes.toBytes("row");
    long timestamp = System.currentTimeMillis();
    Path path = new Path(dir, "temphlog");
    // delete the log if already exists, for test only
    fs.delete(path, true);
    WAL.Writer writer = null;
    WAL.Reader reader = null;
    try {
      HRegionInfo hri = new HRegionInfo(tableName,
          HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW);
      HTableDescriptor htd = new HTableDescriptor(tableName);
      fs.mkdirs(dir);
      // Write log in pb format.
      writer = HLogFactory.createWALWriter(fs, path, conf);
      for (int i = 0; i < recordCount; ++i) {
        HLogKey key = new HLogKey(
            hri.getEncodedNameAsBytes(), tableName, i, timestamp, HConstants.DEFAULT_CLUSTER_ID);
        WALEdit edit = new WALEdit();
        for (int j = 0; j < columnCount; ++j) {
          if (i == 0) {
            htd.addFamily(new HColumnDescriptor("column" + j));
          }
          String value = i + "" + j;
          edit.add(new KeyValue(row, row, row, timestamp, Bytes.toBytes(value)));
        }
        writer.append(new WAL.Entry(key, edit));
      }
      writer.sync();
      if (withTrailer) writer.close();

      // Now read the log using standard means.
      reader = HLogFactory.createReader(fs, path, conf);
      assertTrue(reader instanceof ProtobufLogReader);
      if (withTrailer) {
        assertNotNull(reader.getWALTrailer());
      } else {
        assertNull(reader.getWALTrailer());
      }
      for (int i = 0; i < recordCount; ++i) {
        WAL.Entry entry = reader.next();
        assertNotNull(entry);
        assertEquals(columnCount, entry.getEdit().size());
        assertArrayEquals(hri.getEncodedNameAsBytes(), entry.getKey().getEncodedRegionName());
        assertEquals(tableName, entry.getKey().getTablename());
        int idx = 0;
        for (Cell val : entry.getEdit().getCells()) {
          assertTrue(Bytes.equals(row, val.getRow()));
          String value = i + "" + idx;
          assertArrayEquals(Bytes.toBytes(value), val.getValue());
          idx++;
        }
      }
      WAL.Entry entry = reader.next();
      assertNull(entry);
    } finally {
      if (writer != null) {
        writer.close();
      }
      if (reader != null) {
        reader.close();
      }
    }
  }

  //  Impl specific test methods
  /**
   * Tests how the logs are cleaned with rolling, etc.
   * @throws Exception
   */
  abstract void testLogCleaning() throws Exception;

  /**
   * Tests the log file comparator, used to sort the log files.
   * @throws Exception
   */
  abstract void testHLogComparator() throws Exception;

  /**
   * Tests how the WAL archiving scheme is working.
   * @throws IOException
   */
  abstract void testWALArchiving() throws IOException;

  /**
   * Tests that the impl returns correct memstores to flush when looking at the old wal.
   * When the number of WALs reached value defined by hbase.regionserver.maxlogs, the log
   * should try to flush regions so that older WAL files become eligible for archiving. Tests
   * whether the implementation is returning the right regions to flush.
   */
  abstract void testFindMemStoresEligibleForFlush() throws Exception;

  /**
   * Tests IO fencing.
   * @throws IOException
   */
  @Test
  public abstract void testFailedToCreateHLogIfParentRenamed() throws IOException;

  /**
   * helper method to simulate region flush for a WAL.
   * @param hlog
   * @param regionEncodedName
   */
  protected void flushRegion(WALService hlog, byte[] regionEncodedName) {
    hlog.startCacheFlush(regionEncodedName);
    hlog.completeCacheFlush(regionEncodedName);
  }

  static class DumbWALActionsListener implements WALActionsListener {
    int increments = 0;

    @Override
    public void visitLogEntryBeforeWrite(HRegionInfo info, HLogKey logKey,
                                         WALEdit logEdit) {
      increments++;
    }

    @Override
    public void visitLogEntryBeforeWrite(HTableDescriptor htd, HLogKey logKey, WALEdit logEdit) {
      //To change body of implemented methods use File | Settings | File Templates.
      increments++;
    }

    @Override
    public void preLogRoll(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void postLogRoll(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void preLogArchive(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void postLogArchive(Path oldFile, Path newFile) {
      // TODO Auto-generated method stub
    }

    @Override
    public void logRollRequested() {
      // TODO Auto-generated method stub

    }

    @Override
    public void logCloseRequested() {
      // not interested
    }
  }

}

