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
package org.apache.hadoop.hbase.mob;

import java.io.IOException;
import java.lang.Thread;
import java.util.Arrays;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import org.apache.hadoop.hbase.master.cleaner.TimeToLiveHFileCleaner;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.testclassification.IntegrationTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
Repro MOB data loss

 1. Settings: Region Size 200 MB,  Flush threshold 800 KB.
 2. Insert 10 Million records
 3. MOB Compaction and Archiver
      a) Trigger MOB Compaction (every 2 minutes)
      b) Trigger major compaction (every 2 minutes)
      c) Trigger archive cleaner (every 3 minutes)
 4. Validate MOB data after complete data load.

 */
@Category(IntegrationTests.class)
public class IntegrationTestMobCompaction {
  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTestMobCompaction.class);

  private static final HBaseTestingUtility HTU = new HBaseTestingUtility();

  private final static String famStr = "f1";
  private final static byte[] fam = Bytes.toBytes(famStr);
  private final static byte[] qualifier = Bytes.toBytes("q1");
  private final static long mobLen = 10;
  private final static byte[] mobVal = Bytes.toBytes("01234567890123456789012345678901234567890123456789012345678901234567890123456789");
  private final static HTableDescriptor hdt = HTU.createTableDescriptor("testMobCompactTable");
  private static HColumnDescriptor hcd= new HColumnDescriptor(fam);
  private final static long count = 10000000;
  private static Table table = null;

  private static volatile boolean run = true;

  @BeforeClass
  public static void beforeClass() throws Exception {
    LOG.info("setting up.");
    HTU.getConfiguration().setInt("hfile.format.version", 3);
    HTU.getConfiguration().setLong(TimeToLiveHFileCleaner.TTL_CONF_KEY, 0);
    HTU.getConfiguration().setInt("hbase.client.retries.number", 100);
    HTU.getConfiguration().setInt("hbase.hregion.max.filesize", 200000000);
    HTU.getConfiguration().setInt("hbase.hregion.memstore.flush.size", 800000);
    //HTU.getConfiguration().setInt("hbase.mob.compaction.chore.period", 0);
    HTU.startMiniCluster();

    hcd= new HColumnDescriptor(fam);
    hcd.setMobEnabled(true);
    hcd.setMobThreshold(mobLen);
    hcd.setMaxVersions(1);
    hdt.addFamily(hcd);
    table = HTU.createTable(hdt, null);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    LOG.info("cleaning up.");
    try {
        HTU.getHBaseAdmin().disableTable(hdt.getTableName());
        HTU.deleteTable(hdt.getTableName());
    } catch (Exception E) {
      LOG.error("failed to remove test table {}", hdt.getTableName());
    }
    HTU.shutdownMiniCluster();
  }

  class MobCompactionThread implements Runnable {
     @Override
     public void run() {
       while (run) {
         try {
           LOG.info("Requesting MOB Compaction on table {}, family {}", hdt.getTableName(), famStr);
           HTU.getHBaseAdmin().compactMob(hdt.getTableName(), fam);
           Thread.sleep(120000);
         } catch (Exception e) {
           LOG.error("MOB Compaction Thread failed.", e);
         }
       }
     }
  }

  class MajorCompaction implements Runnable {
    @Override
    public void run() {
      while (run) {
        try {
          LOG.info("Requesting major compaction table {}, family {}", hdt.getTableName(), famStr);
          HTU.getHBaseAdmin().majorCompact(hdt.getTableName(), fam);
          Thread.sleep(120000);
        } catch (Exception e) {
          LOG.error("Major Compaction thread failed.", e);
        }
      }
    }
  }

  class CleanArchive implements Runnable {
    @Override
    public void run() {
      while (run) {
        try {
          LOG.info("Requesting hfile cleaner chore");
          HTU.getMiniHBaseCluster().getMaster().getHFileCleaner().choreForTesting();
          Thread.sleep(180000);
        } catch (Exception e) {
          LOG.error("Cleaner chore thread failed.", e);
        }
      }
    }
  }

  class WriteData implements Runnable {

    private long rows=-1;

    public WriteData(long rows) {
      this.rows = rows;
    }

    @Override
    public void run() {
      try {
        LOG.info("starting to write data.");
        HRegion r = HTU.getMiniHBaseCluster().getRegions(hdt.getTableName()).get(0);
        //Put Operation
        for (int i = 0; i < rows; i++) {
          Put p = new Put(Bytes.toBytes(i));
          p.addColumn(fam, qualifier, mobVal);
          table.put(p);
          if (i % 200000 == 0) {
            LOG.info("Finished writing row number {}", i);
          }
        }
        run = false;
      }catch (Exception e) {
        LOG.error("data writing thread failed.", e);
      }
    }
  }

  @Test
  public void testMobCompaction() throws InterruptedException, IOException {
    Thread writeData = new Thread(new WriteData(count));
    writeData.start();

    Thread mobcompact = new Thread(new MobCompactionThread());
    mobcompact.start();

    Thread majorcompact = new Thread(new MajorCompaction());
    majorcompact.start();

    Thread cleanArchive = new Thread(new CleanArchive());
    cleanArchive.start();

    while (run){
      Thread.sleep(30000);
    }
    LOG.info("validating written data.");
    int i = 0;
    try {
      Get get;
      Result result;
      for (; i < count ; i++) {
        get = new Get(Bytes.toBytes(i));
        result = table.get(get);
        assertTrue(Arrays.equals(result.getValue(fam, qualifier), mobVal));
        if (i % 200000 == 0) {
          LOG.info("finished checking row {}", i);
        }
      }
    } catch (Exception e) {
      LOG.error("failed to validate data on row {}", i, e);
      fail("failed to validate data on row " + i);
    }
  }
}
