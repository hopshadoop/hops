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
package org.apache.hadoop.net;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p>
 * Simple {@link DNSToSwitchMapping} implementation that reads a 2 column text
 * file. The columns are separated by whitespace. The first column is a DNS or
 * IP address and the second column specifies the rack where the address maps.
 * </p>
 * <p>
 * This class uses the configuration parameter {@code
 * net.topology.table.file.name} to locate the mapping file.
 * </p>
 * <p>
 * Calls to {@link #resolve(List)} will look up the address as defined in the
 * mapping file. If no entry corresponding to the address is found, the value
 * {@code /default-rack} is returned.
 * </p>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class TableMapping extends CachedDNSToSwitchMapping {

  private static final Logger LOG = LoggerFactory.getLogger(TableMapping.class);
  
  public TableMapping() {
    super(new RawTableMapping());
  }
  
  private RawTableMapping getRawMapping() {
    return (RawTableMapping) rawMapping;
  }

  @Override
  public Configuration getConf() {
    return getRawMapping().getConf();
  }

  @Override
  public void setConf(Configuration conf) {
    super.setConf(conf);
    getRawMapping().setConf(conf);
  }
  
  @Override
  public void reloadCachedMappings() {
    super.reloadCachedMappings();
    getRawMapping().reloadCachedMappings();
  }
  
  private static final class RawTableMapping extends Configured
      implements DNSToSwitchMapping {
    
    private Map<String, String> map;
  
    private Map<String, String> load() {
      Map<String, String> loadMap = new HashMap<String, String>();
  
      String filename = getConf().get(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY, null);
      if (StringUtils.isBlank(filename)) {
        LOG.warn(NET_TOPOLOGY_TABLE_MAPPING_FILE_KEY + " not configured. ");
        return null;
      }
  

      try (BufferedReader reader =
               new BufferedReader(new InputStreamReader(
                   new FileInputStream(filename), StandardCharsets.UTF_8))) {
        String line = reader.readLine();
        while (line != null) {
          line = line.trim();
          if (line.length() != 0 && line.charAt(0) != '#') {
            String[] columns = line.split("\\s+");
            if (columns.length == 2) {
              loadMap.put(columns[0], columns[1]);
            } else {
              LOG.warn("Line does not have two columns. Ignoring. " + line);
            }
          }
          line = reader.readLine();
        }
      } catch (Exception e) {
        LOG.warn(filename + " cannot be read.", e);
        return null;
      }
      return loadMap;
    }
  
    @Override
    public synchronized List<String> resolve(List<String> names) {
      if (map == null) {
        map = load();
        if (map == null) {
          LOG.warn("Failed to read topology table. " +
            NetworkTopology.DEFAULT_RACK + " will be used for all nodes.");
          map = new HashMap<String, String>();
        }
      }
      List<String> results = new ArrayList<String>(names.size());
      for (String name : names) {
        String result = map.get(name);
        if (result != null) {
          results.add(result);
        } else {
          results.add(NetworkTopology.DEFAULT_RACK);
        }
      }
      return results;
    }

    @Override
    public void reloadCachedMappings() {
      Map<String, String> newMap = load();
      if (newMap == null) {
        LOG.error("Failed to reload the topology table.  The cached " +
            "mappings will not be cleared.");
      } else {
        synchronized(this) {
          map = newMap;
        }
      }
    }

    @Override
    public void reloadCachedMappings(List<String> names) {
      // TableMapping has to reload all mappings at once, so no chance to 
      // reload mappings on specific nodes
      reloadCachedMappings();
    }
  }
}
