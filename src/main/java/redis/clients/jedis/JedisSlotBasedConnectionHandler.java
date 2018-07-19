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

import java.util.List;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.util.Set;

import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.Protocol;
import redis.clients.util.JedisClusterCRC16;

/**
 * The standard connection handler for a Redis cluster.
 */
public class JedisSlotBasedConnectionHandler extends JedisClusterConnectionHandler {

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, int timeout) {
    this(nodes, poolConfig, timeout, timeout);
  }

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
      final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, null);
  }

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, password, Protocol.DEFAULT_DATABASE);
  }

  public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password, int database) {
    super(nodes, poolConfig, connectionTimeout, soTimeout, password, database);
    initializeSlotsCache(nodes, poolConfig, password, database);
  }

  @Override
  Jedis getConnection(int keyCount, byte[]... keys) {
    if (keys == null || keys.length == 0) {
      throw new JedisClusterException("No way to dispatch this command to Redis Cluster.");
    } else {
      // Get slot.
      int slot = JedisClusterCRC16.getSlot(keys[0]);
      for (int i = 1; i < keyCount; i++) {
        int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
        if (slot != nextSlot) {
          throw new JedisClusterException("No way to dispatch this command to Redis Cluster "
              + "because keys have different slots.");
        }
      }

      // Get connection from slot.
      JedisPool connectionPool = cache.getSlotPool(slot);
      if (connectionPool != null) {
        // We can't guarantee getting a valid connection because of node assignment
        return connectionPool.getResource();
      } else {
        refreshNodesCache(); // It's an abnormal situation for cluster mode that we have just nothing
                             // for a slot, try to rediscover state.
        connectionPool = cache.getSlotPool(slot);
        if (connectionPool != null) {
          return connectionPool.getResource();
        } else {
          // No choice, fallback to new connection to random node
          return getConnection();
        }
      }
    }
  }

  @Override
  void refreshNodesCache() {
    cache.renewClusterSlots(null);
  }

  @Override
  void refreshNodesCache(Jedis jedis) {
    cache.renewClusterSlots(jedis);
  }

  @Override
  public Jedis getConnection() {
    // In antirez's redis-rb-cluster implementation,
    // getRandomConnection always return valid connection (able to
    // ping-pong)
    // or exception if all connections are invalid

    List<JedisPool> pools = cache.getShuffledNodesPool();

    for (JedisPool pool : pools) {
      Jedis jedis = null;
      try {
        jedis = pool.getResource();

        if (jedis == null) {
          continue;
        }

        String result = jedis.ping();

        if (result.equalsIgnoreCase("pong")) return jedis;

        jedis.close();
      } catch (JedisException ex) {
        if (jedis != null) {
          jedis.close();
        }
      }
    }

    throw new JedisNoReachableClusterNodeException("No reachable node in cluster");
  }

  private void initializeSlotsCache(Set<HostAndPort> startNodes, GenericObjectPoolConfig poolConfig, String password, int database) {
    for (HostAndPort hostAndPort : startNodes) {
      Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
      if (password != null) {
        jedis.auth(password);
      }
      if (database != Protocol.DEFAULT_DATABASE) {
        jedis.select(database);
      }
      try {
        List<Object> slots = jedis.clusterSlots();
        cache.discoverClusterNodesAndSlots(slots);
        break;
      } catch (JedisConnectionException e) {
        // try next nodes
      } finally {
        if (jedis != null) {
          jedis.close();
        }
      }
    }
  }
}
