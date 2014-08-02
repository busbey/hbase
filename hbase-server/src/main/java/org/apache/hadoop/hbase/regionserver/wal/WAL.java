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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.io.Writable;

/**
 * The Write Ahead Log (WAL) stores all the edits to the HStore.
 * This interface provides APIs, reader and writer abstractions for all WAL implementors.
 * <p>
 * A WAL file is rolled once it reaches a HDFS block size.
 * See {@link FSHLog} for an example implementation.
 */
@InterfaceAudience.Private
public interface WAL {

  interface Reader{
    /**
     * @param fs File system.
     * @param path Path.
     * @param c Configuration.
     * @param s Input stream that may have been pre-opened by the caller; may be null.
     */
    void init(FileSystem fs, Path path, Configuration c, FSDataInputStream s) throws IOException;

    void close() throws IOException;

    Entry next() throws IOException;

    Entry next(Entry reuse) throws IOException;

    void seek(long pos) throws IOException;

    long getPosition() throws IOException;
    void reset() throws IOException;

    /**
     * @return the WALTrailer of the current HLog. It may be null in case of legacy or corrupt WAL
     *         files.
     */
    // TODO: What we need a trailer on WAL for?
    WALTrailer getWALTrailer();
  }

  interface Writer {
    void init(FileSystem fs, Path path, Configuration c, boolean overwritable) throws IOException;

    void close() throws IOException;

    void sync() throws IOException;

    void append(Entry entry) throws IOException;

    long getLength() throws IOException;

    /**
     * Sets HLog's WALTrailer. This trailer is appended at the end of WAL on closing.
     * @param walTrailer trailer to append to WAL.
     */
    void setWALTrailer(WALTrailer walTrailer);
  }

  /**
   * Configuration name of HLog Trailer's warning size. If a waltrailer's size is greater than the
   * configured size, a warning is logged. This is used with Protobuf reader/writer.
   */
  public static final String WAL_TRAILER_WARN_SIZE = "hbase.regionserver.waltrailer.warn.size";
  public static final int DEFAULT_WAL_TRAILER_WARN_SIZE = 1024 * 1024; // 1MB
  public static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  public static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  public static final String SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** File Extension used while splitting an HLog into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";
  public static final boolean SPLIT_SKIP_ERRORS_DEFAULT = false;
  /** The hbase:meta region's HLog filename extension */
  public static final String META_HLOG_FILE_EXTN = ".meta";
  public static final long NO_SEQUENCE_ID = -1;

  /**
   * @return the total number of WAL files (including rolled WALs).
   */
  int getNumLogFiles();

  /**
   * returns the number of rolled WAL files.
   */
  int getNumRolledLogFiles();

  /**
   * @return the path of the current WAL file.
   */
  Path getCurrentFileName();

  /**
   * @return the current size of all HLog files (including rolled files).
   */
  long getLogFileSize();

  /**
   * For notification post append to the writer.  Used by metrics system at least.
   * @param entry
   * @param elapsedTime
   * @return Size of this append.
   */
  long postAppend(final Entry entry, final long elapsedTime);

  /**
   * For notification post writer sync.  Used by metrics system at least.
   * @param timeInMillis How long the filesystem sync took in milliseconds.
   * @param handlerSyncs How many sync handler calls were released by this call to filesystem
   * sync.
   */
  void postSync(final long timeInMillis, final int handlerSyncs);

  /**
   * @return the number of entries in the current WAL file
   */
  int getNumEntries();

  /**
   * Get LowReplication-Roller status
   * @return lowReplicationRollEnabled
   */
  boolean isLowReplicationRollEnabled();

  /**
   * Utility class that lets us keep track of the edit with it's key.
   * Only used when splitting logs.
   */
  // TODO: Remove this Writable.
  class Entry implements Writable {
    private WALEdit edit;
    private HLogKey key;

    public Entry() {
      edit = new WALEdit();
      key = new HLogKey();
    }

    /**
     * Constructor for both params
     *
     * @param edit log's edit
     * @param key log's key
     */
    public Entry(HLogKey key, WALEdit edit) {
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
    public HLogKey getKey() {
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

    @Override
    @SuppressWarnings("deprecation")
    public void write(DataOutput dataOutput) throws IOException {
      this.key.write(dataOutput);
      this.edit.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      this.key.readFields(dataInput);
      this.edit.readFields(dataInput);
    }
  }

}
