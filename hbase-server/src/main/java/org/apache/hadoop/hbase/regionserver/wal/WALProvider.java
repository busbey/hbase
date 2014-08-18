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

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;

/**
 * The Write Ahead Log (WAL) stores all the edits to the HStore.
 * This interface provides the entry point for all WAL implementors.
 * <p>
 * See {@link DefaultWALProvider} for an example implementation.
 *
 * A single WALProvider will be used for retrieving multiple WALs in a particular region server
 * and must be threadsafe.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public interface WALProvider {

  /**
   * Set up the provider to create wals.
   * will only be called once per instance.
   * @param factory factory that made us may not be null
   * @param conf may not be null
   * @param listeners may be null
   * @param providerId differentiate between providers from one factory. may be null
   */
  void init(final WALFactory factory, final Configuration conf,
      final List<WALActionsListener> listeners, final String providerId) throws IOException;

  /**
   * @param identifier may not be null. contents will not be altered.
   * @return a WAL for writing entries for the given region.
   */
  WAL getWAL(final byte[] identifier) throws IOException;

  /**
   * Close outstanding WALs.
   */
  void close() throws IOException;

  /**
   * Close outstanding WALs and clean up any persisted state.
   */
  void closeAndDelete() throws IOException;

  interface Reader extends Closeable {
    Entry next() throws IOException;

    Entry next(Entry reuse) throws IOException;

    void seek(long pos) throws IOException;

    long getPosition() throws IOException;
    void reset() throws IOException;

    /**
     * @return the WALTrailer of the current WAL. It may be null in case of legacy or corrupt WAL
     *         files.
     */
    // TODO: What we need a trailer on WAL for?
    WALTrailer getWALTrailer();
  }

  // TODO Why would someone every reference a Writer directly instead of a WAL?
  interface Writer extends Closeable {
    void sync() throws IOException;

    void append(Entry entry) throws IOException;

    long getLength() throws IOException;

    /**
     * Sets WAL's WALTrailer. This trailer is appended at the end of WAL on closing.
     * @param walTrailer trailer to append to WAL.
     */
    void setWALTrailer(WALTrailer walTrailer);
  }

  /**
   * Configuration name of WAL Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, providers should log a warning. e.g. this is used with Protobuf reader/writer.
   */
  public static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  public static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB

  /**
   * Utility class that lets us keep track of the edit with it's key.
   * Only used when splitting logs.
   */
  class Entry {
    private WALEdit edit;
    private WALKey key;

    public Entry() {
      edit = new WALEdit();
      key = new WALKey();
    }

    /**
     * Constructor for both params
     *
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(WALKey key, WALEdit edit) {
      super();
      this.key = key;
      this.edit = edit;
    }

    /**
     * Gets the edit
     *
     * @return edit
     */
    public WALEdit getEdit() {
      return edit;
    }

    /**
     * Gets the key
     *
     * @return key
     */
    public WALKey getKey() {
      return key;
    }

    /**
     * Set compression context for this entry.
     *
     * @param compressionContext
     *          Compression context
     */
    public void setCompressionContext(CompressionContext compressionContext) {
      edit.setCompressionContext(compressionContext);
      key.setCompressionContext(compressionContext);
    }

    @Override
    public String toString() {
      return this.key + "=" + this.edit;
    }

  }

}
