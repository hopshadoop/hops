/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.hadoop.hdfs.server.namenode;

import com.google.common.base.Charsets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.security.PrivilegedExceptionAction;

/**
 * Renew delegation tokens over http for use in hftp.
 */
@SuppressWarnings("serial")
public class RenewDelegationTokenServlet extends DfsServlet {
  private static final Log LOG =
      LogFactory.getLog(RenewDelegationTokenServlet.class);
  public static final String PATH_SPEC = "/renewDelegationToken";
  public static final String TOKEN = "token";
  
  @Override
  protected void doGet(final HttpServletRequest req,
      final HttpServletResponse resp) throws ServletException, IOException {
    final UserGroupInformation ugi;
    final ServletContext context = getServletContext();
    final Configuration conf = NameNodeHttpServer.getConfFromContext(context);
    try {
      ugi = getUGI(req, conf);
    } catch (IOException ioe) {
      LOG.info("Request for token received with no authentication from " +
          req.getRemoteAddr(), ioe);
      resp.sendError(HttpServletResponse.SC_FORBIDDEN,
          "Unable to identify or authenticate user");
      return;
    }
    final NameNode nn = NameNodeHttpServer.getNameNodeFromContext(context);
    String tokenString = req.getParameter(TOKEN);
    if (tokenString == null) {
      resp.sendError(HttpServletResponse.SC_MULTIPLE_CHOICES,
          "Token to renew not specified");
    }
    final Token<DelegationTokenIdentifier> token =
        new Token<>();
    token.decodeFromUrlString(tokenString);
    
    try {
      long result = ugi.doAs(new PrivilegedExceptionAction<Long>() {
        @Override
        public Long run() throws Exception {
          return nn.getRpcServer().renewDelegationToken(token);
        }
      });
      final PrintWriter os = new PrintWriter(
          new OutputStreamWriter(resp.getOutputStream(), Charsets.UTF_8));
      os.println(result);
      os.close();
    } catch (Exception e) {
      // transfer exception over the http
      String exceptionClass = e.getClass().getName();
      String exceptionMsg = e.getLocalizedMessage();
      String strException = exceptionClass + ";" + exceptionMsg;
      LOG.info("Exception while renewing token. Re-throwing. s=" + strException,
          e);
      resp.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
          strException);
    }
  }
}
