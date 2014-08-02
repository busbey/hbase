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

import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.annotations.VisibleForTesting;
/**
 * The skeleton WAL implementation, which provides methods and attributes common to all concrete
 * implementations.
 * <p>
 */
@InterfaceAudience.Private
public abstract class AbstractWAL implements WAL, WALService, Syncable {
  /**
   * file system instance
   */
  final protected FileSystem fs;
  /**
   * HBase root directory.
   */
  final protected Path rootDir;
  /**
   * WAL directory, where all WAL files would be placed.
   */
  final protected Path fullPathLogDir;
  /**
   * conf object
   */
  final protected Configuration conf;
  // Listeners that are called on WAL events.
  final protected List<WALActionsListener> listeners = new CopyOnWriteArrayList<WALActionsListener>();

  /**
   * dir path where old logs are kept.
   */
  final protected Path fullPathOldLogDir;

  /**
   * block size of the underlying Filesystem. Used when to do the roll.
   */
  final protected long blocksize;

  /**
   * Prefix of a WAL file, usually the region server name it is hosted on.
   */
  final protected String logFilePrefix;

  protected WALCoprocessorHost coprocessorHost;
  /**
   * Is the WAL is closed
   */
  protected volatile boolean closed = false;
  /**
   * If the WAL is for hbase:meta region
   */
  final protected boolean forMeta;

  /**
   * Constructor to instantiate all the required fields of the WAL.
   * @param fs
   * @param root
   * @param logDir
   * @param oldLogDir
   * @param conf
   * @param listeners
   * @param failIfLogDirExists
   * @param prefix
   * @param forMeta
   * @throws IOException
   */
  protected AbstractWAL(FileSystem fs, Path root, String logDir, String oldLogDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfLogDirExists,
      String prefix, boolean forMeta) throws IOException {
    this.fs = fs;
    this.rootDir = root;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathOldLogDir = new Path(rootDir, oldLogDir);
    this.forMeta = forMeta;
    this.conf = conf;
    this.blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
      FSUtils.getDefaultBlockSize(this.fs, this.fullPathLogDir));

    boolean dirExists = false;
    if (failIfLogDirExists && (dirExists = this.fs.exists(fullPathLogDir))) {
      throw new IOException("Target HLog directory already exists: " + fullPathLogDir);
    }
    if (!dirExists && !fs.mkdirs(fullPathLogDir)) {
      throw new IOException("Unable to mkdir " + fullPathLogDir);
    }

    if (!fs.exists(this.fullPathOldLogDir)) {
      if (!fs.mkdirs(this.fullPathOldLogDir)) {
        throw new IOException("Unable to mkdir " + this.fullPathOldLogDir);
      }
    }

    // Register listeners.  TODO: Should this exist anymore?  We have CPs?
    if (listeners != null) {
      for (WALActionsListener i: listeners) {
        registerWALActionsListener(i);
      }
    }

    // If prefix is null||empty then just name it hlog
    this.logFilePrefix =
      prefix == null || prefix.isEmpty() ? "hlog" : URLEncoder.encode(prefix, "UTF8");
    this.coprocessorHost = new WALCoprocessorHost(this, conf);
  }

  @Override
  public void registerWALActionsListener(final WALActionsListener listener) {
    this.listeners.add(listener);
  }

  @Override
  public boolean unregisterWALActionsListener(final WALActionsListener listener) {
    return this.listeners.remove(listener);
  }

  /**
   * Get the directory we are making logs in.
   * @return dir
   */
  protected Path getDir() {
    return fullPathLogDir;
  }

  /**
   * @param oldLogDir
   * @param p
   * @return archive path of a WAL file
   */
  static Path getHLogArchivePath(Path oldLogDir, Path p) {
    return new Path(oldLogDir, p.getName());
  }

  static String formatRecoveredEditsFileName(final long seqid) {
    return String.format("%019d", seqid);
  }

  @Override
  public WALCoprocessorHost getCoprocessorHost() {
    return coprocessorHost;
  }

  /**
   * Find the 'getNumCurrentReplicas' on the passed <code>os</code> stream.
   * This is used for getting current replicas of a file being written. It is used by all all
   * concrete WAL implementations, and by tests (see TestLogRolling).
   * @return Method or null.
   */
  protected static Method getGetNumCurrentReplicas(final FSDataOutputStream os) {
    // TODO: Remove all this and use the now publically available
    // HdfsDataOutputStream#getCurrentBlockReplication()
    Method m = null;
    if (os != null) {
      Class<? extends OutputStream> wrappedStreamClass = os.getWrappedStream().getClass();
      try {
        m = wrappedStreamClass.getDeclaredMethod("getNumCurrentReplicas", new Class<?>[] {});
        m.setAccessible(true);
      } catch (NoSuchMethodException e) {
        LOG.info("FileSystem's output stream doesn't support getNumCurrentReplicas; " +
         "HDFS-826 not available; fsOut=" + wrappedStreamClass.getName());
      } catch (SecurityException e) {
        LOG.info("No access to getNumCurrentReplicas on FileSystems's output stream; HDFS-826 " +
          "not available; fsOut=" + wrappedStreamClass.getName(), e);
        m = null; // could happen on setAccessible()
      }
    }
    if (m != null) {
      if (LOG.isTraceEnabled()) LOG.trace("Using getNumCurrentReplicas");
    }
    return m;
  }

  /**
   * Currently, we need to expose the writer's OutputStream to tests so that they can manipulate
   * the default behavior (such as setting the maxRecoveryErrorCount value for example (see
   * {@link TestWALReplay#testReplayEditsWrittenIntoWAL()}). This is done using reflection on the
   * underlying HDFS OutputStream. All the implementors need to pass the stream to these tests.
   * NOTE: This could be removed once Hadoop1 support is removed.
   * @VisibleForTesting
   */
  abstract OutputStream getOutputStream();

}
