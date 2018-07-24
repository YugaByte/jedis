/**
 *   The following only applies to changes made to this file as part of YugaByte development.
 *
 *      Portions Copyright (c) YugaByte, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 *   except in compliance with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software distributed under the
 *   License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 *   either express or implied.  See the License for the specific language governing permissions
 *   and limitations under the License.
 */

package redis.clients.jedis;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

/**
 * Abstraction for handling the node-selection strategy (e.g. load-balancing) for a cluster.
 * Generically maintains a cache of cluster information but node-selection and cache-refresh logic
 * are supposed to be implemented by the subclasses.
 */
public abstract class JedisClusterConnectionHandler implements Closeable {

  protected final Set<HostAndPort> contactPoints;
  protected final String password;
  protected final JedisClusterInfoCache cache;

  public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
                                       final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, String database) {
    this.contactPoints = nodes;
    this.password = password;
    this.cache = new JedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password, database);
  }

  /**
   * Get a connection to random node from the cluster.
   *
   * @return the Jedis connection
   */
  abstract Jedis getConnection();

  /**
   * Get a connection to a node that can handle queries on these keys.
   * If no such node exists this should throw a JedisClusterException.
   *
   * @return The Jedis connection
   */
  abstract Jedis getConnection(int keyCount, byte[]... keys);

  /**
   * Refresh the information about nodes and slots from the cache.
   * If needed to connect to a node (e.g. to send a 'CLUSTER NODES' command,
   * get a random connection.
   */
  abstract void refreshNodesCache();

  /**
   * Refresh the information about nodes and slots from the cache.
   * If needed to connect to a node (e.g. to send a 'CLUSTER NODES' command,
   * use the given Jedis connection.
   */
  abstract void refreshNodesCache(Jedis jedis);

  public Jedis getConnectionFromNode(HostAndPort node) {
    return cache.setupNodeIfNotExist(node).getResource();
  }
  
  public Map<String, JedisPool> getNodes() {
    return cache.getNodes();
  }

  @Override
  public void close() {
    cache.reset();
  }
}
