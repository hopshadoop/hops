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
package org.apache.hadoop.security.ssl;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.util.EnumSet;
import java.util.Set;

/**
 * Abstract class for CRL fetchers. Provides method to dump CRL to a file in the local filesystem
 */
public abstract class CRLFetcherAbstr implements CRLFetcher {
  private final static Set<PosixFilePermission> permissions = EnumSet.of(
      PosixFilePermission.OWNER_READ,
      PosixFilePermission.OWNER_WRITE,
      PosixFilePermission.OWNER_EXECUTE,
      PosixFilePermission.GROUP_READ,
      PosixFilePermission.GROUP_WRITE,
      PosixFilePermission.GROUP_EXECUTE,
      PosixFilePermission.OTHERS_READ,
      PosixFilePermission.OTHERS_EXECUTE);
  
  protected URI inputURI;
  private URI outputURI;
  private Path outputPath;
  private boolean permissionsSet = false;
  
  public CRLFetcherAbstr() {
  }
  
  /**
   * Set input URI for the fetcher, it could be a file or a URL
   * @param inputURI Location of the CRL
   */
  @Override
  public void setInputURI(URI inputURI) {
    this.inputURI = inputURI;
  }
  
  /**
   * Get the output location for the fetcher
   * @return Output location, usually a file in the local filesystem
   */
  @Override
  public URI getOutputURI() {
    return outputURI;
  }
  
  /**
   * Set the output URI for the fetcher, usually a file in the local filesystem
   * @param outputURI Output location
   */
  @Override
  public void setOutputURI(URI outputURI) {
    this.outputURI = outputURI;
    this.outputPath = Paths.get(outputURI);
  }
  
  /**
   * Write a CRL to a file in the local filesystem
   * @param sourceChannel Input source
   * @throws FileNotFoundException
   * @throws IOException
   */
  protected void writeToFile(ReadableByteChannel sourceChannel) throws FileNotFoundException, IOException {
    FileOutputStream outputStream = new FileOutputStream(outputPath.toFile());
    outputStream.getChannel().transferFrom(sourceChannel, 0, Long.MAX_VALUE);
    if (!permissionsSet) {
      permissionsSet = true;
      Set<PosixFilePermission> filePermissions = Files.getPosixFilePermissions(outputPath, LinkOption.NOFOLLOW_LINKS);
      // More permissions is fine, but less is problematic
      if (!filePermissions.containsAll(permissions)) {
        Files.setPosixFilePermissions(outputPath, permissions);
      }
    }
  }
}
