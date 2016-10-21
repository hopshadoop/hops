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

package org.apache.hadoop.util;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * A helper to load the native hadoop code i.e. libhadoop.so.
 * This handles the fallback to either the bundled libhadoop-Linux-i386-32.so
 * or the default java implementations where appropriate.
 *  
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public final class NativeCodeLoader {

  private static final Log LOG =
    LogFactory.getLog(NativeCodeLoader.class);
  
  private static boolean nativeCodeLoaded = false;
  
  static {
    // Try to load native hadoop library and set fallback flag appropriately
    if(LOG.isDebugEnabled()) {
      LOG.warn("Trying to load the custom-built native-hadoop library...");
    }
    try {
      System.loadLibrary("hadoop");
      LOG.warn("Loaded the native-hadoop library");
      nativeCodeLoaded = true;
    } catch (Throwable t) {
        System.out.println(t);
              String javaLibPath = System.getProperty("java.library.path");
        System.out.println(javaLibPath);
      // Ignore failure to load
      if(LOG.isDebugEnabled()) {
        System.out.println(t);
        LOG.warn("Failed to load native-hadoop with error: " + t);
        LOG.warn("java.library.path=" +
            System.getProperty("java.library.path"));
      }
    }
    
    if (!nativeCodeLoaded) {
      LOG.warn("Unable to load native-hadoop library for your platform... " +
               "using builtin-java classes where applicable");
    }
  }

  private NativeCodeLoader() {}

  /**
   * Check if native-hadoop code is loaded for this platform.
   * 
   * @return <code>true</code> if native-hadoop is loaded, 
   *         else <code>false</code>
   */
  public static boolean isNativeCodeLoaded() {
    return nativeCodeLoaded;
  }

  /**
   * Returns true only if this build was compiled with support for snappy.
   */
  public static native boolean buildSupportsSnappy();

  /**
   * Returns true only if this build was compiled with support for ISA-L.
   */
  public static native boolean buildSupportsIsal();

  /**
   * Returns true only if this build was compiled with support for openssl.
   */
  public static native boolean buildSupportsOpenssl();

  public static native String getLibraryName();

}
