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
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Syncable;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.FSUtils;

import com.google.common.annotations.VisibleForTesting;

/**
 * The skeleton WAL implementation, which provides methods and attributes common to all concrete
 * implementations.
 * <p>
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public abstract class AbstractWAL implements WALProvider, WAL, Syncable {
  private final static Log LOG = LogFactory.getLog(AbstractWAL.class);

  public static final Pattern EDITFILES_NAME_PATTERN = Pattern.compile("-?[0-9]+");
  public static final String RECOVERED_LOG_TMPFILE_SUFFIX = ".temp";
  public static final String SEQUENCE_ID_FILE_SUFFIX = "_seqid";
  public static final String WAL_FILE_NAME_DELIMITER = ".";
  /** File Extension used while splitting an WAL into regions (HBASE-2312) */
  public static final String SPLITTING_EXT = "-splitting";
  /** The hbase:meta region's WAL filename extension */
  public static final String META_WAL_FILE_EXTN = ".meta";

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

  /**
   * Suffix included on generated wal file names 
   */
  final protected String logFileSuffix;

  protected WALCoprocessorHost coprocessorHost;
  /**
   * Is the WAL is closed
   */
  protected volatile boolean closed = false;

  /**
   * Constructor to instantiate all the required fields of the WAL.
   * @param fs filesystem handle
   * @param rootDir path to where logs and oldlogs
   * @param logDir dir where wals are stored
   * @param oldLogDir dir where wals are archived
   * @param conf configuration to use
   * @param listeners Listeners on WAL events. Listeners passed here will
   * be registered before we do anything else; e.g. the
   * Constructor {@link #rollWriter()}.
   * @param failIfLogDirExists If true IOException will be thrown if dir already exists.
   * @param prefix should always be hostname and port in distributed env and
   *        it will be URL encoded before being used.
   *        If prefix is null, "wal" will be used
   * @param suffix will be url encoded. null is treated as empty
   */
  protected AbstractWAL(FileSystem fs, Path root, String logDir, String oldLogDir,
      Configuration conf, List<WALActionsListener> listeners, boolean failIfLogDirExists,
      String prefix, String suffix) throws IOException {
    this.fs = fs;
    this.rootDir = root;
    this.fullPathLogDir = new Path(rootDir, logDir);
    this.fullPathOldLogDir = new Path(rootDir, oldLogDir);
    this.conf = conf;
    this.blocksize = this.conf.getLong("hbase.regionserver.hlog.blocksize",
      FSUtils.getDefaultBlockSize(this.fs, this.fullPathLogDir));

    boolean dirExists = false;
    if (failIfLogDirExists && (dirExists = this.fs.exists(fullPathLogDir))) {
      throw new IOException("Target WAL directory already exists: " + fullPathLogDir);
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

    // If prefix is null||empty then just name it wal
    this.logFilePrefix =
      prefix == null || prefix.isEmpty() ? "wal" : URLEncoder.encode(prefix, "UTF8");
    this.logFileSuffix = (suffix == null) ? "" : URLEncoder.encode(suffix, "UTF8");
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
  protected static Path getWALArchivePath(Path oldLogDir, Path p) {
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
   */
  @VisibleForTesting
  abstract OutputStream getOutputStream();

  /**
   * Pattern used to validate a WAL file name
   */
  private static final Pattern pattern = Pattern.compile(".*\\.\\d*("+META_WAL_FILE_EXTN+")*");

  /**
   * @param filename
   *          name of the file to validate
   * @return <tt>true</tt> if the filename matches an WAL, <tt>false</tt>
   *         otherwise
   */
  public static boolean validateWALFilename(String filename) {
    return pattern.matcher(filename).matches();
  }

  /**
   * Construct the WAL directory name
   *
   * @param serverName
   *          Server name formatted as described in {@link ServerName}
   * @return the relative WAL directory name, e.g.
   *         <code>.logs/1.example.org,60030,12345</code> if
   *         <code>serverName</code> passed is
   *         <code>1.example.org,60030,12345</code>
   */
  public static String getWALDirectoryName(final String serverName) {
    StringBuilder dirName = new StringBuilder(HConstants.HREGION_LOGDIR_NAME);
    dirName.append("/");
    dirName.append(serverName);
    return dirName.toString();
  }

  /**
   * @param regiondir
   *          This regions directory in the filesystem.
   * @return The directory that holds recovered edits files for the region
   *         <code>regiondir</code>
   */
  public static Path getRegionDirRecoveredEditsDir(final Path regiondir) {
    return new Path(regiondir, HConstants.RECOVERED_EDITS_DIR);
  }

  /**
   * Move aside a bad edits file.
   *
   * @param fs
   * @param edits
   *          Edits file to move aside.
   * @return The name of the moved aside file.
   * @throws IOException
   */
  public static Path moveAsideBadEditsFile(final FileSystem fs, final Path edits)
      throws IOException {
    Path moveAsideName = new Path(edits.getParent(), edits.getName() + "."
        + System.currentTimeMillis());
    if (!fs.rename(edits, moveAsideName)) {
      LOG.warn("Rename failed from " + edits + " to " + moveAsideName);
    }
    return moveAsideName;
  }

  /**
   * @param path
   *          - the path to analyze. Expected format, if it's in wal directory:
   *          / [base directory for hbase] / hbase / .logs / ServerName /
   *          logfile
   * @return null if it's not a log file. Returns the ServerName of the region
   *         server that created this log file otherwise.
   */
  public static ServerName getServerNameFromWALDirectoryName(
      Configuration conf, String path) throws IOException {
    if (path == null
        || path.length() <= HConstants.HREGION_LOGDIR_NAME.length()) {
      return null;
    }

    if (conf == null) {
      throw new IllegalArgumentException("parameter conf must be set");
    }

    final String rootDir = conf.get(HConstants.HBASE_DIR);
    if (rootDir == null || rootDir.isEmpty()) {
      throw new IllegalArgumentException(HConstants.HBASE_DIR
          + " key not found in conf.");
    }

    final StringBuilder startPathSB = new StringBuilder(rootDir);
    if (!rootDir.endsWith("/"))
      startPathSB.append('/');
    startPathSB.append(HConstants.HREGION_LOGDIR_NAME);
    if (!HConstants.HREGION_LOGDIR_NAME.endsWith("/"))
      startPathSB.append('/');
    final String startPath = startPathSB.toString();

    String fullPath;
    try {
      fullPath = FileSystem.get(conf).makeQualified(new Path(path)).toString();
    } catch (IllegalArgumentException e) {
      LOG.info("Call to makeQualified failed on " + path + " " + e.getMessage());
      return null;
    }

    if (!fullPath.startsWith(startPath)) {
      return null;
    }

    final String serverNameAndFile = fullPath.substring(startPath.length());

    if (serverNameAndFile.indexOf('/') < "a,0,0".length()) {
      // Either it's a file (not a directory) or it's not a ServerName format
      return null;
    }

    Path p = new Path(path);
    return getServerNameFromWALDirectoryName(p);
  }

  /**
   * This function returns region server name from a log file name which is in either format:
   * hdfs://<name node>/hbase/.logs/<server name>-splitting/... or hdfs://<name
   * node>/hbase/.logs/<server name>/...
   * @param logFile
   * @return null if the passed in logFile isn't a valid WAL file path
   */
  public static ServerName getServerNameFromWALDirectoryName(Path logFile) {
    Path logDir = logFile.getParent();
    String logDirName = logDir.getName();
    if (logDirName.equals(HConstants.HREGION_LOGDIR_NAME)) {
      logDir = logFile;
      logDirName = logDir.getName();
    }
    ServerName serverName = null;
    if (logDirName.endsWith(SPLITTING_EXT)) {
      logDirName = logDirName
          .substring(0, logDirName.length() - SPLITTING_EXT.length());
    }
    try {
      serverName = ServerName.parseServerName(logDirName);
    } catch (IllegalArgumentException ex) {
      serverName = null;
      LOG.warn("Cannot parse a server name from path=" + logFile + "; " + ex.getMessage());
    }
    if (serverName != null && serverName.getStartcode() < 0) {
      LOG.warn("Invalid log file path=" + logFile);
      return null;
    }
    return serverName;
  }

  /**
   * Returns sorted set of edit files made by wal-log splitter, excluding files
   * with '.temp' suffix.
   *
   * @param fs
   * @param regiondir
   * @return Files in passed <code>regiondir</code> as a sorted set.
   * @throws IOException
   */
  public static NavigableSet<Path> getSplitEditFilesSorted(final FileSystem fs,
      final Path regiondir) throws IOException {
    NavigableSet<Path> filesSorted = new TreeSet<Path>();
    Path editsdir = getRegionDirRecoveredEditsDir(regiondir);
    if (!fs.exists(editsdir))
      return filesSorted;
    FileStatus[] files = FSUtils.listStatus(fs, editsdir, new PathFilter() {
      @Override
      public boolean accept(Path p) {
        boolean result = false;
        try {
          // Return files and only files that match the editfile names pattern.
          // There can be other files in this directory other than edit files.
          // In particular, on error, we'll move aside the bad edit file giving
          // it a timestamp suffix. See moveAsideBadEditsFile.
          Matcher m = EDITFILES_NAME_PATTERN.matcher(p.getName());
          result = fs.isFile(p) && m.matches();
          // Skip the file whose name ends with RECOVERED_LOG_TMPFILE_SUFFIX,
          // because it means splitwal thread is writting this file.
          if (p.getName().endsWith(RECOVERED_LOG_TMPFILE_SUFFIX)) {
            result = false;
          }
        } catch (IOException e) {
          LOG.warn("Failed isFile check on " + p);
        }
        return result;
      }
    });
    if (files == null)
      return filesSorted;
    for (FileStatus status : files) {
      filesSorted.add(status.getPath());
    }
    return filesSorted;
  }

  public static boolean isMetaFile(Path p) {
    return isMetaFile(p.getName());
  }

  public static boolean isMetaFile(String p) {
    if (p != null && p.endsWith(META_WAL_FILE_EXTN)) {
      return true;
    }
    return false;
  }

}
