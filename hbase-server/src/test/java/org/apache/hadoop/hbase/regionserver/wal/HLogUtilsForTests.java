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

import org.apache.hadoop.fs.Path;

/**
 * An Utility testcase that returns the number of log files that
 * were rolled to be accessed from outside packages.
 * 
 * This class makes available methods that are package protected.
 *  This is interesting for test only.
 */
public class HLogUtilsForTests {

  /**
   * 
   * @param log
   * @return
   */
  public static int getNumRolledLogFiles(WALService log) {
    return ((AbstractWAL) log).getNumRolledLogFiles();
  }

  public static int getNumEntries(WALService log) {
    return ((AbstractWAL) log).getNumEntries();
  }

  /**
   * A WAL file name is of the format: 
   * <server-name>{@link WAL#WAL_FILE_NAME_DELIMITER}<file-creation-timestamp>[.meta].
   * It returns the file create timestamp from the file name.
   * @return the file number that is part of the WAL file name
   */
  public static long extractFileNumFromPath(Path hlogName) {
    if (hlogName == null) throw new IllegalArgumentException("The HLog path couldn't be null");
    String[] walPathStrs = null;
    String hlogPath = hlogName.toString();
    // if it is a meta wal file, it would have a -meta prefix at the end.
    boolean metaWAL = (hlogPath.endsWith(WAL.META_HLOG_FILE_EXTN)) ? true : false;
    walPathStrs = hlogPath.split("\\" + WAL.WAL_FILE_NAME_DELIMITER);
    if (metaWAL) return Long.parseLong(walPathStrs[walPathStrs.length - 2]);
    return Long.parseLong(walPathStrs[walPathStrs.length - 1]);
  }
}
