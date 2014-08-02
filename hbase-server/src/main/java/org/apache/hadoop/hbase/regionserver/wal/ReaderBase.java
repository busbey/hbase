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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.util.LRUDictionary;
import org.apache.hadoop.hbase.protobuf.generated.WALProtos.WALTrailer;
import org.apache.hadoop.hbase.util.FSUtils;

@InterfaceAudience.LimitedPrivate({HBaseInterfaceAudience.COPROC, HBaseInterfaceAudience.PHOENIX})
public abstract class ReaderBase implements WAL.Reader {
  private static final Log LOG = LogFactory.getLog(ReaderBase.class);
  protected Configuration conf;
  protected FileSystem fs;
  protected Path path;
  protected long edit = 0;
  protected long fileLength;
  protected WALTrailer trailer;
  // maximum size of the wal Trailer in bytes. If a user writes/reads a trailer with size larger
  // than this size, it is written/read respectively, with a WARN message in the log.
  protected int trailerWarnSize;
  /**
   * Compression context to use reading.  Can be null if no compression.
   */
  protected CompressionContext compressionContext = null;
  protected boolean emptyCompressionContext = true;

  /**
   * Default constructor.
   */
  public ReaderBase() {
  }

  @Override
  public void init(FileSystem fs, Path path, Configuration conf, FSDataInputStream stream)
      throws IOException {
    this.conf = conf;
    this.path = path;
    this.fs = fs;
    this.fileLength = this.fs.getFileStatus(path).getLen();
    this.trailerWarnSize = conf.getInt(WAL.WAL_TRAILER_WARN_SIZE,
      WAL.DEFAULT_WAL_TRAILER_WARN_SIZE);
    String cellCodecClsName = initReader(stream);

    boolean compression = hasCompression();
    if (compression) {
      // If compression is enabled, new dictionaries are created here.
      try {
        if (compressionContext == null) {
          compressionContext = new CompressionContext(LRUDictionary.class,
              FSUtils.isRecoveredEdits(path), hasTagCompression());
        } else {
          compressionContext.clear();
        }
      } catch (Exception e) {
        throw new IOException("Failed to initialize CompressionContext", e);
      }
    }
    initAfterCompression(cellCodecClsName);
  }

  @Override
  public WAL.Entry next() throws IOException {
    return next(null);
  }

  @Override
  public WAL.Entry next(WAL.Entry reuse) throws IOException {
    WAL.Entry e = reuse;
    if (e == null) {
      e = new WAL.Entry(new HLogKey(), new WALEdit());
    }
    if (compressionContext != null) {
      e.setCompressionContext(compressionContext);
    }

    boolean hasEntry = false;
    try {
      hasEntry = readNext(e);
    } catch (IllegalArgumentException iae) {
      TableName tableName = e.getKey().getTablename();
      if (tableName != null && tableName.equals(TableName.OLD_ROOT_TABLE_NAME)) {
        // It is old ROOT table edit, ignore it
        LOG.info("Got an old ROOT edit, ignoring ");
        return next(e);
      }
      else throw iae;
    }
    edit++;
    if (compressionContext != null && emptyCompressionContext) {
      emptyCompressionContext = false;
    }
    return hasEntry ? e : null;
  }

  @Override
  public void seek(long pos) throws IOException {
    if (compressionContext != null && emptyCompressionContext) {
      while (next() != null) {
        if (getPosition() == pos) {
          emptyCompressionContext = false;
          break;
        }
      }
    }
    seekOnFs(pos);
  }

  /**
   * Initializes the log reader with a particular stream (may be null).
   * Reader assumes ownership of the stream if not null and may use it. Called once.
   * @return the class name of cell Codec, null if such information is not available
   */
  protected abstract String initReader(FSDataInputStream stream) throws IOException;

  /**
   * Initializes the compression after the shared stuff has been initialized. Called once.
   */
  protected abstract void initAfterCompression() throws IOException;
  
  /**
   * Initializes the compression after the shared stuff has been initialized. Called once.
   * @param cellCodecClsName class name of cell Codec
   */
  protected abstract void initAfterCompression(String cellCodecClsName) throws IOException;
  /**
   * @return Whether compression is enabled for this log.
   */
  protected abstract boolean hasCompression();

  /**
   * @return Whether tag compression is enabled for this log.
   */
  protected abstract boolean hasTagCompression();

  /**
   * Read next entry.
   * @param e The entry to read into.
   * @return Whether there was anything to read.
   */
  protected abstract boolean readNext(WAL.Entry e) throws IOException;

  /**
   * Performs a filesystem-level seek to a certain position in an underlying file.
   */
  protected abstract void seekOnFs(long pos) throws IOException;

  @Override
  public WALTrailer getWALTrailer() {
    return null;
  }
}
