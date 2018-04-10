// Copyright (c) YugaByte, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
// in compliance with the License.  You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied.  See the License for the specific language governing permissions and limitations
// under the License.
//

package redis.clients.jedis;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * A connection handler that just returns a random node every time.
 * It does not do any node discovery by itself, but regularly refreshes the node list in the
 * background by (re)resolving the inet addresses of the contact point(s).
 * So it should pick up changes to DNS entries on the fly.
 */
public class JedisRandomNodeConnectionHandler extends JedisClusterConnectionHandler {

  private static long DEFAULT_NODES_REFRESH_INTERVAL_MILLIS = 10000;
  private boolean includeOnlyLiveNodes;
  private Random random = new Random(System.currentTimeMillis());
  private Thread thread = null;

  public JedisRandomNodeConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
                                          int connectionTimeout, int soTimeout) {
    this(nodes, poolConfig, connectionTimeout, soTimeout, /* includeOnlyLiveNodes */ true,
        DEFAULT_NODES_REFRESH_INTERVAL_MILLIS);
  }

  public JedisRandomNodeConnectionHandler(Set<HostAndPort> nodes, GenericObjectPoolConfig poolConfig,
                                          int connectionTimeout, int soTimeout,
                                          boolean includeOnlyLiveNodes, final long refreshIntervalMillis) {
    super(nodes, poolConfig, connectionTimeout, soTimeout, null);
    this.includeOnlyLiveNodes = includeOnlyLiveNodes;
    refreshNodesCache();

    // Start a background thread to regularly refresh the nodes cache.
    Runnable r = new Runnable() {
      public void run() {
        while (true) {
          try {
            Thread.sleep(refreshIntervalMillis);
            refreshNodesCache();
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      }
    };
    thread = new Thread(r);
    thread.start();
  }

  @Override
  Jedis getConnection(int keyCount, byte[]... keys) {
    // Ignore the keys and return a random connection.
    return getConnection();
  }

  @Override
  Jedis getConnection() {
    return cache.getNode(cache.getRandomNodeKey(random)).getResource();
  }

  @Override
  void refreshNodesCache() {
     Set<HostAndPort> nodes = new HashSet<HostAndPort>();
     // Try to resolve the contact points again.
     for (HostAndPort hostAndPort : contactPoints) {
      try {
        for (InetAddress address : InetAddress.getAllByName(hostAndPort.getHost())) {
          HostAndPort node = new HostAndPort(address.getHostAddress(), hostAndPort.getPort());
          nodes.add(node);
        }
      } catch (UnknownHostException e) {
        // throw
        e.printStackTrace();
      }
    }
    cache.renewClusterNodes(nodes, includeOnlyLiveNodes);
  }

  @Override
  void refreshNodesCache(Jedis jedis) {
    refreshNodesCache();
  }

  @Override
  public void close() {
    super.close();
    if (thread != null) {
      try {
        thread.interrupt();
        thread.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
}
