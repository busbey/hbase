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
package org.apache.hadoop.hbase.mapreduce;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.regionserver.wal.WAL;
import org.apache.hadoop.mapreduce.InputFormat;

/**
 * Simple {@link InputFormat} for {@link WAL} files.
 * @deprecated use {@link WALInputFormat}
 */
@Deprecated
@InterfaceAudience.Public
public class HLogInputFormat extends WALInputFormat {
  private static final Log LOG = LogFactory.getLog(HLogInputFormat.class);

  /* XXX Only needed if we fix the key names in WALInputFormat
  public static final String START_TIME_KEY = "hlog.start.time";
  public static final String END_TIME_KEY = "hlog.end.time";
   */
}
