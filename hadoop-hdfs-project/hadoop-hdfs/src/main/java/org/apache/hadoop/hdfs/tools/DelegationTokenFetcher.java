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
package org.apache.hadoop.hdfs.tools;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenSecretManager;

import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.WebHdfsFileSystem;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;
import java.security.PrivilegedExceptionAction;
import java.util.Collection;
import java.util.Date;
import org.apache.hadoop.util.ExitUtil;
import com.google.common.annotations.VisibleForTesting;

/**
 * Fetch a DelegationToken from the current Namenode and store it in the
 * specified file.
 */
@InterfaceAudience.Private
public class DelegationTokenFetcher {
  private static final String WEBSERVICE = "webservice";
  private static final String CANCEL = "cancel";
  private static final String HELP = "help";
  private static final String HELP_SHORT = "h";
  private static final Log LOG = LogFactory
      .getLog(DelegationTokenFetcher.class);
  private static final String PRINT = "print";
  private static final String RENEW = "renew";
  private static final String RENEWER = "renewer";

  /**
   * Command-line interface
   */
  public static void main(final String[] args) throws Exception {
    final Configuration conf = new HdfsConfiguration();
    Options fetcherOptions = new Options();
    fetcherOptions
      .addOption(WEBSERVICE, true, "HTTP url to reach the NameNode at")
      .addOption(RENEWER, true, "Name of the delegation token renewer")
      .addOption(CANCEL, false, "cancel the token")
      .addOption(RENEW, false, "renew the token")
      .addOption(PRINT, false, "print the token")
      .addOption(HELP_SHORT, HELP, false, "print out help information");

    GenericOptionsParser parser = new GenericOptionsParser(conf,
            fetcherOptions, args);
    CommandLine cmd = parser.getCommandLine();

    final String webUrl = cmd.hasOption(WEBSERVICE) ? cmd
            .getOptionValue(WEBSERVICE) : null;
    final String renewer = cmd.hasOption(RENEWER) ? cmd.getOptionValue
            (RENEWER) : null;
    final boolean cancel = cmd.hasOption(CANCEL);
    final boolean renew = cmd.hasOption(RENEW);
    final boolean print = cmd.hasOption(PRINT);
    final boolean help = cmd.hasOption(HELP);
    String[] remaining = parser.getRemainingArgs();

    // check option validity
    if (help) {
      printUsage(System.out);
      System.exit(0);
    }

    int commandCount = (cancel ? 1 : 0) + (renew ? 1 : 0) + (print ? 1 : 0);
    if (commandCount > 1) {
      System.err.println("ERROR: Only specify cancel, renew or print.");
      printUsage(System.err);
    }
    if (remaining.length != 1 || remaining[0].charAt(0) == '-') {
      System.err.println("ERROR: Must specify exacltly one token file");
      printUsage(System.err);
    }
    // default to using the local file system
    FileSystem local = FileSystem.getLocal(conf);
    final Path tokenFile = new Path(local.getWorkingDirectory(), remaining[0]);

    // Login the current user
    UserGroupInformation.getCurrentUser().doAs(new PrivilegedExceptionAction<Object>() {
      @Override
      public Object run() throws Exception {
        if (print) {
          printTokens(conf, tokenFile);
        } else if (cancel) {
          cancelTokens(conf, tokenFile);
        } else if (renew) {
          renewTokens(conf, tokenFile);
        } else {
          // otherwise we are fetching
          FileSystem fs = getFileSystem(conf, webUrl);
          saveDelegationToken(conf, fs, renewer, tokenFile);
        }
        return null;
      }
    });
  }

  private static FileSystem getFileSystem(Configuration conf, String url)
          throws IOException {
    if (url == null) {
      return FileSystem.get(conf);
    }

    // For backward compatibility
    URI fsUri = URI.create(
            url.replaceFirst("^http://", WebHdfsFileSystem.SCHEME + "://")
               .replaceFirst("^https://", SWebHdfsFileSystem.SCHEME + "://"));

    return FileSystem.get(fsUri, conf);
  }

  @VisibleForTesting
  static void cancelTokens(final Configuration conf, final Path tokenFile)
          throws IOException, InterruptedException {
    for (Token<?> token : readTokens(tokenFile, conf)) {
      if (token.isManaged()) {
        token.cancel(conf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Cancelled token for " + token.getService());
        }
      }
    }
  }

  @VisibleForTesting
  static void renewTokens(final Configuration conf, final Path tokenFile)
          throws IOException, InterruptedException {
    for (Token<?> token : readTokens(tokenFile, conf)) {
      if (token.isManaged()) {
        long result = token.renew(conf);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Renewed token for " + token.getService() + " until: " +
                  new Date(result));
        }
      }
    }
  }

  @VisibleForTesting
  static void saveDelegationToken(Configuration conf, FileSystem fs,
                                  final String renewer, final Path tokenFile)
          throws IOException {
    Token<?> token = fs.getDelegationToken(renewer);

    Credentials cred = new Credentials();
    cred.addToken(token.getKind(), token);
    cred.writeTokenStorageFile(tokenFile, conf);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Fetched token " + fs.getUri() + " for " + token.getService()
              + " into " + tokenFile);
    }
  }

  private static void printTokens(final Configuration conf,
                                  final Path tokenFile)
          throws IOException {
    DelegationTokenIdentifier id = new DelegationTokenSecretManager(0, 0, 0,
            0, null).createIdentifier();
    for (Token<?> token : readTokens(tokenFile, conf)) {
      DataInputStream in = new DataInputStream(new ByteArrayInputStream(token
              .getIdentifier()));
      id.readFields(in);
      System.out.println("Token (" + id + ") for " + token.getService());
    }
  }

  private static void printUsage(PrintStream err) {
    err.println("fetchdt retrieves delegation tokens from the NameNode");
    err.println();
    err.println("fetchdt <opts> <token file>");
    err.println("Options:");
    err.println("  --webservice <url>  Url to contact NN on (starts with " +
            "http:// or https://)");
    err.println("  --renewer <name>    Name of the delegation token renewer");
    err.println("  --cancel            Cancel the delegation token");
    err.println("  --renew             Renew the delegation token.  " +
            "Delegation " + "token must have been fetched using the --renewer" +
            " <name> option.");
    err.println("  --print             Print the delegation token");
    err.println();
    GenericOptionsParser.printGenericCommandUsage(err);
    ExitUtil.terminate(1);
  }

  private static Collection<Token<?>> readTokens(Path file, Configuration conf)
          throws IOException {
    Credentials creds = Credentials.readTokenStorageFile(file, conf);
    return creds.getAllTokens();
  }
}
