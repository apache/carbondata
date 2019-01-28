package org.apache.carbondata.store;

import org.apache.carbondata.sdk.file.CarbonReader;
import py4j.GatewayServer;

import java.io.IOException;

public class gateWay {
  public int addition(int first, int second) {
    return first + second;
  }

  public CarbonReader read(String path) throws IOException, InterruptedException {
    CarbonReader reader = CarbonReader
        .builder(path, "_temp")
        .build();
    return reader;
  }

  public static void main(String[] args) {
    gateWay app = new gateWay();
    // app is now the gateway.entry_point
    GatewayServer server = new GatewayServer(app);
    server.start();
  }
}