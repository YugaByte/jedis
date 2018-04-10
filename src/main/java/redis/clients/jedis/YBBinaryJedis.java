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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class YBBinaryJedis extends BinaryJedisCluster implements AdvancedBinaryJedisCommands {

  public YBBinaryJedis() {
    this(Protocol.DEFAULT_HOST);
  }

  public YBBinaryJedis(final String host) {
    this(host, Protocol.DEFAULT_PORT);
  }

  public YBBinaryJedis(final String host, final int port) {
    this(host, port, Protocol.DEFAULT_TIMEOUT);
  }

  public YBBinaryJedis(final String host, final int port, final int timeout) {
    this(host, port, timeout, timeout);
  }

  public YBBinaryJedis(final String host, final int port, final int connectionTimeout,
                       final int soTimeout) {
    this(new HashSet<HostAndPort>(Collections.singletonList(new HostAndPort(host, port))),
        connectionTimeout, soTimeout, DEFAULT_MAX_REDIRECTIONS, new GenericObjectPoolConfig());
  }

  public YBBinaryJedis(Set<HostAndPort> contactPoints, int connectionTimeout, int soTimeout,
                       int maxAttempts, GenericObjectPoolConfig poolConfig) {
    super(new JedisSlotBasedConnectionHandler(contactPoints, poolConfig, connectionTimeout, soTimeout), maxAttempts);
  }

  @Override
  public void close() {
    if (connectionHandler != null) {
      connectionHandler.close();
    }
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
}
