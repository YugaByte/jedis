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

import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.SafeEncoder;

public abstract class JedisClusterCommand<T> {

  private JedisClusterConnectionHandler connectionHandler;
  private int maxAttempts;
  private ThreadLocal<Jedis> askConnection = new ThreadLocal<Jedis>();

  public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
    this.connectionHandler = connectionHandler;
    this.maxAttempts = maxAttempts;
  }

  public abstract T execute(Jedis connection);

  public T run() {
    return runWithRetries(this.maxAttempts, false, false, 0);
  }

  public T run(String key) {
    if (key == null) {
      return run();
    }

    return runWithRetries(this.maxAttempts, false, false, 1, SafeEncoder.encode(key));
  }

  public T run(int keyCount, String... keys) {
    if (keys == null || keys.length == 0) {
      return run();
    }

    byte[][] encodedKeys = new byte[keys.length][];
    for (int i = 0; i < keys.length; i++) {
      encodedKeys[i] = SafeEncoder.encode(keys[i]);
    }

    return runWithRetries(this.maxAttempts, false, false, keyCount, encodedKeys);
  }

  public T runBinary(byte[] key) {
    if (key == null) {
      return run();
    }

    return runWithRetries(this.maxAttempts, false, false, 1, key);
  }

  public T runBinary(int keyCount, byte[]... keys) {
    if (keys == null || keys.length == 0) {
      return run();
    }

    return runWithRetries(this.maxAttempts, false, false, keyCount, keys);
  }

  public T runWithAnyNode() {
    Jedis connection = null;
    try {
      connection = connectionHandler.getConnection();
      return execute(connection);
    } catch (JedisConnectionException e) {
      throw e;
    } finally {
      releaseConnection(connection);
    }
  }

  private T runWithRetries(int attempts, boolean tryRandomNode, boolean asking, int keyCount, byte[]... keys) {
    if (attempts <= 0) {
      throw new JedisClusterMaxRedirectionsException("Too many Cluster redirections?");
    }

    Jedis connection = null;
    try {

      if (asking) {
        // TODO: Pipeline asking with the original command to make it
        // faster....
        connection = askConnection.get();
        connection.asking();

        // if asking success, reset asking flag
        asking = false;
      } else {
        if (tryRandomNode) {
          connection = connectionHandler.getConnection();
        } else {
          connection = connectionHandler.getConnection(keyCount, keys);
        }
      }

      return execute(connection);

    } catch (JedisNoReachableClusterNodeException jnrcne) {
      throw jnrcne;
    } catch (JedisConnectionException jce) {
      // release current connection before recursion
      releaseConnection(connection);
      connection = null;

      if (attempts <= 1) {
        //We need this because if node is not reachable anymore - we need to finally initiate slots renewing,
        //or we can stuck with cluster state without one node in opposite case.
        //But now if maxAttempts = 1 or 2 we will do it too often. For each time-outed request.
        //TODO make tracking of successful/unsuccessful operations for node - do renewing only
        //if there were no successful responses from this node last few seconds
        this.connectionHandler.refreshNodesCache();

        //no more redirections left, throw original exception, not JedisClusterMaxRedirectionsException, because it's not MOVED situation
        throw jce;
      }

      return runWithRetries(attempts - 1, tryRandomNode, asking, keyCount, keys);
    } catch (JedisRedirectionException jre) {
      // if MOVED redirection occurred,
      if (jre instanceof JedisMovedDataException) {
        // it rebuilds cluster's slot cache
        // recommended by Redis cluster specification
        this.connectionHandler.refreshNodesCache(connection);
      }

      // release current connection before recursion or renewing
      releaseConnection(connection);
      connection = null;

      if (jre instanceof JedisAskDataException) {
        asking = true;
        askConnection.set(this.connectionHandler.getConnectionFromNode(jre.getTargetNode()));
      } else if (jre instanceof JedisMovedDataException) {
      } else {
        throw new JedisClusterException(jre);
      }

      return runWithRetries(attempts - 1, false, asking, keyCount, keys);
    } finally {
      releaseConnection(connection);
    }
  }

  private void releaseConnection(Jedis connection) {
    if (connection != null) {
      connection.close();
    }
  }

}
