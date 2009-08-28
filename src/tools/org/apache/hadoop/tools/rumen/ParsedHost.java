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
package org.apache.hadoop.tools.rumen;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class ParsedHost {
  String rackName;
  String nodeName;

  private static Pattern splitPattern = Pattern
      .compile("/([0-9\\\\\\.]+)/(.+)");

  static int numberOfDistances() {
    return 3;
  }

  /**
   * @return the components, broadest first [ie., the last element is always the
   *         individual node name]
   */
  String[] nameComponents() {
    String[] result = new String[2];

    result[0] = rackName;
    result[1] = nodeName;

    return result;
  }

  String nameComponent(int i) throws IllegalArgumentException {
    switch (i) {
    case 0:
      return rackName;

    case 1:
      return nodeName;

    default:
      throw new IllegalArgumentException(
          "Host location component index out of range.");
    }
  }

  @Override
  public int hashCode() {
    return rackName.hashCode() * 17 + nodeName.hashCode();
  }

  ParsedHost(String name) throws IllegalArgumentException {
    // separate out the node name
    Matcher matcher = splitPattern.matcher(name);

    if (!matcher.matches()) {
      throw new IllegalArgumentException("Illegal node designator: \"" + name
          + "\"");
    }

    rackName = matcher.group(1);
    nodeName = matcher.group(2);
  }

  public ParsedHost(LoggedLocation loc) {
    List<String> coordinates = loc.getLayers();

    rackName = coordinates.get(0);
    nodeName = coordinates.get(1);
  }

  LoggedLocation makeLoggedLocation() {
    LoggedLocation result = new LoggedLocation();

    List<String> coordinates = new ArrayList<String>();

    coordinates.add(rackName);
    coordinates.add(nodeName);

    result.setLayers(coordinates);

    return result;
  }

  // expects the broadest name first
  ParsedHost(String[] names) {
    rackName = names[0];
    nodeName = names[1];
  }

  // returns the broadest name first
  String[] nameList() {
    String[] result = new String[2];

    result[0] = rackName;
    result[1] = nodeName;

    return result;
  }

  public boolean equals(Object other) {
    if (other instanceof ParsedHost) {
      return distance((ParsedHost) other) == 0;
    }

    return false;
  }

  int distance(ParsedHost other) {
    if (nodeName.equals(other.nodeName)) {
      return 0;
    }

    if (rackName.equals(other.rackName)) {
      return 1;
    }

    return 2;
  }
}
