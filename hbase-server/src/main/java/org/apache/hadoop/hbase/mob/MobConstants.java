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
package org.apache.hadoop.hbase.mob;

import org.apache.hadoop.hbase.ArrayBackedTag;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.Tag;
import org.apache.hadoop.hbase.TagType;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * The constants used in mob.
 */
@InterfaceAudience.Public
public final class MobConstants {

  public static final String MOB_SCAN_RAW = "hbase.mob.scan.raw";
  public static final String MOB_CACHE_BLOCKS = "hbase.mob.cache.blocks";
  public static final String MOB_SCAN_REF_ONLY = "hbase.mob.scan.ref.only";
  public static final String EMPTY_VALUE_ON_MOBCELL_MISS = "empty.value.on.mobcell.miss";

  public static final String MOB_FILE_CACHE_SIZE_KEY = "hbase.mob.file.cache.size";
  public static final int DEFAULT_MOB_FILE_CACHE_SIZE = 1000;

  public static final String MOB_DIR_NAME = "mobdir";
  public static final String MOB_REGION_NAME = ".mob";
  public static final byte[] MOB_REGION_NAME_BYTES = Bytes.toBytes(MOB_REGION_NAME);

  public static final String MOB_CLEANER_PERIOD = "hbase.master.mob.cleaner.period";
  public static final int DEFAULT_MOB_CLEANER_PERIOD = 24 * 60 * 60; // one day

  public static final String MOB_CACHE_EVICT_PERIOD = "hbase.mob.cache.evict.period";
  public static final String MOB_CACHE_EVICT_REMAIN_RATIO = "hbase.mob.cache.evict.remain.ratio";
  public static final Tag MOB_REF_TAG = new ArrayBackedTag(TagType.MOB_REFERENCE_TAG_TYPE,
      HConstants.EMPTY_BYTE_ARRAY);

  public static final float DEFAULT_EVICT_REMAIN_RATIO = 0.5f;
  public static final long DEFAULT_MOB_CACHE_EVICT_PERIOD = 3600L;

  public final static String TEMP_DIR_NAME = ".tmp";

  /**
   * The max number of a MOB table regions that is allowed in a batch of the mob compaction.
   * By setting this number to a custom value, users can control the overall effect
   * of a major compaction of a large MOB-enabled table.
   */

  public static final String MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE =
    "hbase.mob.major.compaction.region.batch.size";

  /**
   * Default is 0 - means no limit - all regions of a MOB table will be compacted at once
   */

  public static final int DEFAULT_MOB_MAJOR_COMPACTION_REGION_BATCH_SIZE = 0;

  /**
   * The period that MobCompactionChore runs. The unit is second.
   * The default value is one week.
   */
  public static final String MOB_COMPACTION_CHORE_PERIOD =
    "hbase.mob.compaction.chore.period";
  public static final int DEFAULT_MOB_COMPACTION_CHORE_PERIOD =
    24 * 60 * 60 * 7; // a week
  public static final String MOB_COMPACTOR_CLASS_KEY = "hbase.mob.compactor.class";

  /**
   * Mob compaction type: "full", "io_optimized"
   * "full" - run full major compaction (during migration)
   * "io_optimized" - optimized version for use  case with infrequent updates/deletes
   */
  public final static String MOB_COMPACTION_TYPE_KEY = "hbase.mob.compaction.type";

  public final static String DEFAULT_MOB_COMPACTION_TYPE = "full";

  public final static String IO_OPTIMIZED_MOB_COMPACTION_TYPE = "optimized";

  public final static String FULL_MOB_COMPACTION_TYPE = "full";


  /**
   * Maximum size of a MOB compaction selection
   */
  public static final String MOB_COMPACTION_MAX_FILE_SIZE_KEY =
      "hbase.mob.compactions.max.file.size";
  /**
   * Default maximum selection size = 1GB
   */
  public static final long DEFAULT_MOB_COMPACTION_MAX_FILE_SIZE = 1024 * 1024 * 1024;

  /**
   * Use this configuration option with caution, only during upgrade procedure
   * to handle missing MOB cells during compaction.
   */
  public static final String MOB_UNSAFE_DISCARD_MISS_KEY = "hbase.unsafe.mob.discard.miss";

  public static final boolean DEFAULT_MOB_DISCARD_MISS = false;

  /**
   * Minimum MOB file age to archive, default (3600000 - 1h)
   */
  public static final String MIN_AGE_TO_ARCHIVE_KEY = "hbase.mob.min.age.archive";

  public static final long DEFAULT_MIN_AGE_TO_ARCHIVE = 3600000; // 1h

  private MobConstants() {

  }
}
