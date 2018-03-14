/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.hops.security;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class HopsUtil {
  
  private static final Log LOG = LogFactory.getLog(HopsUtil.class);
  
  /**
   * Read password for cryptographic material from a file. The file could be
   * either localized in a container or from Hopsworks certificates transient
   * directory.
   * @param passwdFile Location of the password file
   * @return Password to unlock cryptographic material
   * @throws IOException
   */
  public static String readCryptoMaterialPassword(File passwdFile) throws
      IOException {
    
    if (!passwdFile.exists()) {
      throw new FileNotFoundException("File containing crypto material " +
          "password could not be found");
    }
    
    return FileUtils.readFileToString(passwdFile);
  }
}
