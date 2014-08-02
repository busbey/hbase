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
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;

import com.google.common.annotations.VisibleForTesting;

/**
 * A Write Ahead Log (WAL) provides service for reading, writing waledits. This interface provides
 * APIs for WAL users (such as RegionServer) to use the WAL (do append, sync, etc).
 */
@InterfaceAudience.Private
public interface WALService {
  Log LOG = LogFactory.getLog(WALService.class);

  /**
   * Registers WALActionsListener
   *
   * @param listener
   */
  void registerWALActionsListener(final WALActionsListener listener);

  /**
   * Unregisters WALActionsListener
   *
   * @param listener
   */
  boolean unregisterWALActionsListener(final WALActionsListener listener);

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * <p>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  byte[][] rollWriter() throws FailedLogCloseException, IOException;

  /**
   * Roll the log writer. That is, start writing log messages to a new file.
   *
   * <p>
   * The implementation is synchronized in order to make sure there's one rollWriter
   * running at any given time.
   *
   * @param force
   *          If true, force creation of a new writer even if no entries have
   *          been written to the current writer
   * @return If lots of logs, flush the returned regions so next time through we
   *         can clean logs. Returns null if nothing to flush. Names are actual
   *         region names as returned by {@link HRegionInfo#getEncodedName()}
   * @throws org.apache.hadoop.hbase.regionserver.wal.FailedLogCloseException
   * @throws IOException
   */
  byte[][] rollWriter(boolean force) throws FailedLogCloseException, IOException;

  /**
   * Shut down the log.
   *
   * @throws IOException
   */
  void close() throws IOException;

  /**
   * Shut down the log and delete the log directory.
   * Used by tests only and in rare cases where we need a log just temporarily while bootstrapping
   * a region or running migrations.
   *
   * @throws IOException
   */
  void closeAndDelete() throws IOException;

  /**
   * Same as {@link #appendNoSync(HRegionInfo, TableName, WALEdit, List, long, HTableDescriptor,
   *   AtomicLong, boolean, long, long)}
   * except it causes a sync on the log
   * @param info
   * @param tableName
   * @param edits
   * @param now
   * @param htd
   * @param sequenceId
   * @throws IOException
   * @deprecated For tests only and even then, should use
   * {@link #appendNoSync(HTableDescriptor, HRegionInfo, HLogKey, WALEdit, AtomicLong, boolean,
   * List)} and {@link #sync()} instead.
   */
  @Deprecated
  @VisibleForTesting
  public void append(HRegionInfo info, TableName tableName, WALEdit edits,
      final long now, HTableDescriptor htd, AtomicLong sequenceId) throws IOException;

  /**
   * Append a set of edits to the WAL. WAL edits are keyed by (encoded) regionName, rowname, and
   * log-sequence-id. The WAL is not flushed/sync'd after this transaction completes BUT on return
   * this edit must have its region edit/sequence id assigned else it messes up our unification
   * of mvcc and sequenceid.
   * @param info
   * @param tableName
   * @param edits
   * @param clusterIds
   * @param now
   * @param htd
   * @param sequenceId A reference to the atomic long the <code>info</code> region is using as
   * source of its incrementing edits sequence id.  Inside in this call we will increment it and
   * attach the sequence to the edit we apply the WAL.
   * @param isInMemstore Always true except for case where we are writing a compaction completion
   * record into the WAL; in this case the entry is just so we can finish an unfinished compaction
   * -- it is not an edit for memstore.
   * @param nonceGroup
   * @param nonce
   * @return Returns a 'transaction id'.  Do not use. This is an internal implementation detail and
   * cannot be respected in all implementations; i.e. the append/sync machine may or may not be
   * able to sync an explicit edit only (the current default implementation syncs up to the time
   * of the sync call syncing whatever is behind the sync).
   * @throws IOException
   * @deprecated Use {@link #appendNoSync(HTableDescriptor, HRegionInfo, HLogKey, WALEdit, AtomicLong, boolean, List)}
   * instead because you can get back the region edit/sequenceid; it is set into the passed in
   * <code>key</code>.
   */
  @Deprecated
  long appendNoSync(HRegionInfo info, TableName tableName, WALEdit edits,
      List<UUID> clusterIds, final long now, HTableDescriptor htd, AtomicLong sequenceId,
      boolean isInMemstore, long nonceGroup, long nonce) throws IOException;

  /**
   * Append a set of edits to the WAL. The WAL is not flushed/sync'd after this transaction
   * completes BUT on return this edit must have its region edit/sequence id assigned
   * else it messes up our unification of mvcc and sequenceid.  On return <code>key</code> will
   * have the region edit/sequence id filled in.
   * @param info
   * @param key Modified by this call; we add to it this edits region edit/sequence id.
   * @param edits Edits to append. MAY CONTAIN NO EDITS for case where we want to get an edit
   * sequence id that is after all currently appended edits.
   * @param htd
   * @param sequenceId A reference to the atomic long the <code>info</code> region is using as
   * source of its incrementing edits sequence id.  Inside in this call we will increment it and
   * attach the sequence to the edit we apply the WAL.
   * @param inMemstore Always true except for case where we are writing a compaction completion
   * record into the WAL; in this case the entry is just so we can finish an unfinished compaction
   * -- it is not an edit for memstore.
   * @param memstoreCells list of Cells added into memstore
   * @return Returns a 'transaction id' and <code>key</code> will have the region edit/sequence id
   * in it.
   * @throws IOException
   */
  long appendNoSync(HTableDescriptor htd, HRegionInfo info, HLogKey key, WALEdit edits,
      AtomicLong sequenceId, boolean inMemstore, List<Cell> memstoreCells) throws IOException;

  // TODO: Do we need all these versions of sync?
  void hsync() throws IOException;

  void hflush() throws IOException;

  /**
   * Sync what we have in the WAL.
   * @throws IOException
   */
  void sync() throws IOException;

  /**
   * Sync the WAL if the txId was not already sync'd.
   * @param txid Transaction id to sync to.
   * @throws IOException
   */
  void sync(long txid) throws IOException;

  /**
   * WAL keeps track of the sequence numbers that were not yet flushed from memstores
   * in order to be able to do cleanup. This method tells WAL that some region is about
   * to flush memstore.
   *
   * <p>We stash the oldest seqNum for the region, and let the the next edit inserted in this
   * region be recorded in {@link #append(HRegionInfo, TableName, WALEdit, long, HTableDescriptor,
   * AtomicLong)} as new oldest seqnum.
   * In case of flush being aborted, we put the stashed value back; in case of flush succeeding,
   * the seqNum of that first edit after start becomes the valid oldest seqNum for this region.
   *
   * @return true if the flush can proceed, false in case wal is closing (ususally, when server is
   * closing) and flush couldn't be started.
   */
  boolean startCacheFlush(final byte[] encodedRegionName);

  /**
   * Complete the cache flush.
   * @param encodedRegionName Encoded region name.
   */
  void completeCacheFlush(final byte[] encodedRegionName);

  /**
   * Abort a cache flush. Call if the flush fails. Note that the only recovery
   * for an aborted flush currently is a restart of the regionserver so the
   * snapshot content dropped by the failure gets restored to the memstore.v
   * @param encodedRegionName Encoded region name.
   */
  void abortCacheFlush(byte[] encodedRegionName);

  /**
   * @return Coprocessor host.
   */
  WALCoprocessorHost getCoprocessorHost();


  /** Gets the earliest sequence number in the memstore for this particular region.
   * This can serve as best-effort "recent" WAL number for this region.
   * @param encodedRegionName The region to get the number for.
   * @return The number if present, HConstants.NO_SEQNUM if absent.
   */
  long getEarliestMemstoreSeqNum(byte[] encodedRegionName);
}
