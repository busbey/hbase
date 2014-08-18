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

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.CompactionDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.FlushDescriptor;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.RegionEventDescriptor;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.protobuf.TextFormat;

@InterfaceAudience.Private
public class WALUtil {
  static final Log LOG = LogFactory.getLog(WALUtil.class);

  /**
   * Write the marker that a compaction has succeeded and is about to be committed.
   * This provides info to the HMaster to allow it to recover the compaction if
   * this regionserver dies in the middle (This part is not yet implemented). It also prevents
   * the compaction from finishing if this regionserver has already lost its lease on the log.
   * @param sequenceId Used by WAL to get sequence Id for the waledit.
   */
  public static void writeCompactionMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final CompactionDescriptor c, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(c.getTableName().toByteArray());
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tn);
    log.append(htd, info, key, WALEdit.createCompaction(info, c), sequenceId, false, null);
    log.sync();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended compaction marker " + TextFormat.shortDebugString(c));
    }
  }

  /**
   * Write a flush marker indicating a start / abort or a complete of a region flush
   */
  public static long writeFlushMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final FlushDescriptor f, AtomicLong sequenceId, boolean sync) throws IOException {
    TableName tn = TableName.valueOf(f.getTableName().toByteArray());
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.append(htd, info, key, WALEdit.createFlushWALEdit(info, f), sequenceId, false, null);
    if (sync) log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended flush marker " + TextFormat.shortDebugString(f));
    }
    return trx;
  }

  /**
   * Write a region open marker indicating that the region is opened
   */
  public static long writeRegionEventMarker(WAL log, HTableDescriptor htd, HRegionInfo info,
      final RegionEventDescriptor r, AtomicLong sequenceId) throws IOException {
    TableName tn = TableName.valueOf(r.getTableName().toByteArray());
    WALKey key = new WALKey(info.getEncodedNameAsBytes(), tn);
    long trx = log.append(htd, info, key, WALEdit.createRegionEventWALEdit(info, r),
      sequenceId, false, null);
    log.sync(trx);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Appended region event marker " + TextFormat.shortDebugString(r));
    }
    return trx;
  }
  
  /**
   * Create a file with name as region open sequence id
   * 
   * @param fs
   * @param regiondir
   * @param newSeqId
   * @param saftyBumper
   * @return long new sequence Id value
   * @throws IOException
   */
  public static long writeRegionOpenSequenceIdFile(final FileSystem fs, final Path regiondir,
      long newSeqId, long saftyBumper) throws IOException {

    Path editsdir = AbstractWAL.getRegionDirRecoveredEditsDir(regiondir);
    long maxSeqId = 0;
    FileStatus[] files = null;
    if (fs.exists(editsdir)) {
      files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
        @Override
        public boolean accept(Path p) {
          if (p.getName().endsWith(AbstractWAL.SEQUENCE_ID_FILE_SUFFIX)) {
            return true;
          }
          return false;
        }
      });
      if (files != null) {
        for (FileStatus status : files) {
          String fileName = status.getPath().getName();
          try {
            Long tmpSeqId = Long.parseLong(fileName.substring(0, fileName.length()
                    - AbstractWAL.SEQUENCE_ID_FILE_SUFFIX.length()));
            maxSeqId = Math.max(tmpSeqId, maxSeqId);
          } catch (NumberFormatException ex) {
            LOG.warn("Invalid SeqId File Name=" + fileName);
          }
        }
      }
    }
    if (maxSeqId > newSeqId) {
      newSeqId = maxSeqId;
    }
    newSeqId += saftyBumper; // bump up SeqId
    
    // write a new seqId file
    Path newSeqIdFile = new Path(editsdir, newSeqId + AbstractWAL.SEQUENCE_ID_FILE_SUFFIX);
    if (!fs.createNewFile(newSeqIdFile)) {
      throw new IOException("Failed to create SeqId file:" + newSeqIdFile);
    }
    // remove old ones
    if(files != null) {
      for (FileStatus status : files) {
        fs.delete(status.getPath(), false);
      }
    }
    return newSeqId;
  }
  
}
