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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import redis.clients.util.Slowlog;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YBJedis extends JedisCluster implements AdvancedJedisCommands, AdvancedBinaryJedisCommands,
    BasicCommands, ClusterCommands {

  public YBJedis() {
    this(Protocol.DEFAULT_HOST);
  }

  public YBJedis(final String host) {
    this(host, Protocol.DEFAULT_PORT);
  }

  public YBJedis(final String host, final int port) {
    this(host, port, DEFAULT_TIMEOUT);
  }

  public YBJedis(final String host, final int port, final int timeout) {
    this(host, port, timeout, timeout);
  }

  public YBJedis(final String host, final int port, final int connectionTimeout, final int soTimeout) {
    this(new HashSet<HostAndPort>(Collections.singletonList(new HostAndPort(host, port))),
        connectionTimeout, soTimeout, DEFAULT_MAX_REDIRECTIONS, new JedisPoolConfig());
  }

  public YBJedis(final Set<HostAndPort> hosts, final int timeout) {
    this(hosts, timeout, timeout, DEFAULT_MAX_REDIRECTIONS, new JedisPoolConfig());
  }

  public YBJedis(Set<HostAndPort> contactPoints, int connectionTimeout, int soTimeout,
                 int maxAttempts, GenericObjectPoolConfig poolConfig) {
    this(new JedisRandomNodeConnectionHandler(contactPoints, poolConfig, connectionTimeout, soTimeout), maxAttempts);
  }

  public YBJedis(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
    super(connectionHandler, maxAttempts);
  }

  public Pipeline pipelined() {
    return new JedisClusterCommand<Pipeline>(connectionHandler, maxAttempts) {
      @Override
      public Pipeline execute(Jedis connection) {
        return connection.pipelined();
      }
    }.run();
  }

  @Override
  public List<byte[]> configGet(final byte[] pattern) {
    return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
      @Override
      public List<byte[]> execute(Jedis connection) {
        return connection.configGet(pattern);
      }
    }.run();
  }

  @Override
  public byte[] configSet(final byte[] parameter, final byte[] value) {
    return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
      @Override
      public byte[] execute(Jedis connection) {
        return connection.configSet(parameter, value);
      }
    }.run();
  }

  @Override
  public String slowlogReset() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.slowlogReset();
      }
    }.run();
  }

  @Override
  public Long slowlogLen() {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.slowlogLen();
      }
    }.run();
  }

  @Override
  public List<byte[]> slowlogGetBinary() {
    return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
      @Override
      public List<byte[]> execute(Jedis connection) {
        return connection.slowlogGetBinary();
      }
    }.run();
  }

  @Override
  public List<byte[]> slowlogGetBinary(final long entries) {
    return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
      @Override
      public List<byte[]> execute(Jedis connection) {
        return connection.slowlogGetBinary(entries);
      }
    }.run();
  }

  @Override
  public Long objectRefcount(final byte[] key) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.objectRefcount(key);
      }
    }.run();
  }

  @Override
  public byte[] objectEncoding(final byte[] key) {
    return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
      @Override
      public byte[] execute(Jedis connection) {
        return connection.objectEncoding(key);
      }
    }.run();
  }

  @Override
  public Long objectIdletime(final byte[] key) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.objectIdletime(key);
      }
    }.run();
  }

  @Override
  public List<String> configGet(final String pattern) {
    return new JedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
      @Override
      public List<String> execute(Jedis connection) {
        return connection.configGet(pattern);
      }
    }.run();
  }

  @Override
  public String configSet(final String parameter, final String value) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.configSet(parameter, value);
      }
    }.run();
  }

  @Override
  public List<Slowlog> slowlogGet() {
    return new JedisClusterCommand<List<Slowlog>>(connectionHandler, maxAttempts) {
      @Override
      public List<Slowlog> execute(Jedis connection) {
        return connection.slowlogGet();
      }
    }.run();
  }

  @Override
  public List<Slowlog> slowlogGet(final long entries) {
    return new JedisClusterCommand<List<Slowlog>>(connectionHandler, maxAttempts) {
      @Override
      public List<Slowlog> execute(Jedis connection) {
        return connection.slowlogGet(entries);
      }
    }.run();
  }

  @Override
  public Long objectRefcount(final String string) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.objectRefcount(string);
      }
    }.run();
  }

  @Override
  public String objectEncoding(final String string) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.objectEncoding(string);
      }
    }.run();
  }

  @Override
  public Long objectIdletime(final String string) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.objectIdletime(string);
      }
    }.run();
  }

  @Override
  public String clusterNodes() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterNodes();
      }
    }.run();
  }

  @Override
  public String clusterMeet(final String ip, final int port) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterMeet(ip, port);
      }
    }.run();
  }

  @Override
  public String clusterAddSlots(final int... slots) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterAddSlots(slots);
      }
    }.run();
  }

  @Override
  public String clusterDelSlots(final int... slots) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterDelSlots(slots);
      }
    }.run();
  }

  @Override
  public String clusterInfo() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterInfo();
      }
    }.run();
  }

  @Override
  public List<String> clusterGetKeysInSlot(final int slot, final int count) {
    return new JedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
      @Override
      public List<String> execute(Jedis connection) {
        return connection.clusterGetKeysInSlot(slot, count);
      }
    }.run();
  }

  @Override
  public String clusterSetSlotNode(final int slot, final String nodeId) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterSetSlotNode(slot, nodeId);
      }
    }.run();
  }

  @Override
  public String clusterSetSlotMigrating(final int slot, final String nodeId) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterSetSlotMigrating(slot, nodeId);
      }
    }.run();
  }

  @Override
  public String clusterSetSlotImporting(final int slot, final String nodeId) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterSetSlotImporting(slot, nodeId);
      }
    }.run();
  }

  @Override
  public String clusterSetSlotStable(final int slot) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterSetSlotStable(slot);
      }
    }.run();
  }

  @Override
  public String clusterForget(final String nodeId) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterForget(nodeId);
      }
    }.run();
  }

  @Override
  public String clusterFlushSlots() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterFlushSlots();
      }
    }.run();
  }

  @Override
  public Long clusterKeySlot(final String key) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.clusterKeySlot(key);
      }
    }.run();
  }

  @Override
  public Long clusterCountKeysInSlot(final int slot) {
    return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
      @Override
      public Long execute(Jedis connection) {
        return connection.clusterCountKeysInSlot(slot);
      }
    }.run();
  }

  @Override
  public String clusterSaveConfig() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterSaveConfig();
      }
    }.run();
  }

  @Override
  public String clusterReplicate(final String nodeId) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterReplicate(nodeId);
      }
    }.run();
  }

  @Override
  public List<String> clusterSlaves(final String nodeId) {
    return new JedisClusterCommand<List<String>>(connectionHandler, maxAttempts) {
      @Override
      public List<String> execute(Jedis connection) {
        return connection.clusterSlaves(nodeId);
      }
    }.run();
  }

  @Override
  public String clusterFailover() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterFailover();
      }
    }.run();
  }

  @Override
  public List<Object> clusterSlots() {
    return new JedisClusterCommand<List<Object>>(connectionHandler, maxAttempts) {
      @Override
      public List<Object> execute(Jedis connection) {
        return connection.clusterSlots();
      }
    }.run();
  }

  @Override
  public String clusterReset(final Reset resetType) {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.clusterReset(resetType);
      }
    }.run();
  }

  @Override
  public String readonly() {
    return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
      @Override
      public String execute(Jedis connection) {
        return connection.readonly();
      }
    }.run();
  }
}
