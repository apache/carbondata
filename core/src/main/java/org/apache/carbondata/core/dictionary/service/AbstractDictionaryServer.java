/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.core.dictionary.service;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;

import org.apache.commons.lang3.SystemUtils;
import org.apache.log4j.Logger;

public abstract class AbstractDictionaryServer {

  public String findLocalIpAddress(Logger LOGGER) {
    try {
      String defaultIpOverride = System.getenv("SPARK_LOCAL_IP");
      if (defaultIpOverride != null) {
        return defaultIpOverride;
      } else {
        InetAddress address = InetAddress.getLocalHost();
        if (address.isLoopbackAddress()) {
          // Address resolves to something like 127.0.1.1, which happens on Debian; try to find
          // a better address using the local network interfaces
          // getNetworkInterfaces returns ifs in reverse order compared to ifconfig output order
          // on unix-like system. On windows, it returns in index order.
          // It's more proper to pick ip address following system output order.
          Enumeration<NetworkInterface> activeNetworkIFs = NetworkInterface.getNetworkInterfaces();
          List<NetworkInterface> reOrderedNetworkIFs = new ArrayList<NetworkInterface>();
          while (activeNetworkIFs.hasMoreElements()) {
            reOrderedNetworkIFs.add(activeNetworkIFs.nextElement());
          }

          if (!SystemUtils.IS_OS_WINDOWS) {
            Collections.reverse(reOrderedNetworkIFs);
          }

          for (NetworkInterface ni : reOrderedNetworkIFs) {
            Enumeration<InetAddress> inetAddresses = ni.getInetAddresses();
            while (inetAddresses.hasMoreElements()) {
              InetAddress addr = inetAddresses.nextElement();
              if (!addr.isLinkLocalAddress() && !addr.isLoopbackAddress()
                  && addr instanceof Inet4Address) {
                // We've found an address that looks reasonable!
                LOGGER.warn("Your hostname, " + InetAddress.getLocalHost().getHostName()
                    + " resolves to a loopback address: " + address.getHostAddress() + "; using "
                    + addr.getHostAddress() + " instead (on interface " + ni.getName() + ")");
                LOGGER.warn("Set SPARK_LOCAL_IP if you need to bind to another address");
                return addr.getHostAddress();
              }
            }
            LOGGER.warn("Your hostname, " + InetAddress.getLocalHost().getHostName()
                + " resolves to a loopback address: " + address.getHostAddress()
                + ", but we couldn't find any external IP address!");
            LOGGER.warn("Set SPARK_LOCAL_IP if you need to bind to another address");
          }
        }
        return address.getHostAddress();
      }
    } catch (UnknownHostException e) {
      LOGGER.error("do not get local host address:" + e.getMessage());
      throw new RuntimeException(e.getMessage());
    } catch (SocketException e) {
      LOGGER.error("do not get net work interface:" + e.getMessage());
      throw new RuntimeException(e.getMessage());
    }
  }
}
