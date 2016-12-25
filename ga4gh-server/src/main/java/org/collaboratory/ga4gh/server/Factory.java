package org.collaboratory.ga4gh.server;

import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_ADDRESS;
import static org.collaboratory.ga4gh.server.config.ServerConfig.NODE_PORT;

import java.net.InetAddress;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import lombok.SneakyThrows;

public class Factory {

  @SuppressWarnings("resource")
  @SneakyThrows
  public static Client newClient(final String nodeAddress, final int nodePort) {
    return new PreBuiltTransportClient(Settings.EMPTY)
        .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(nodeAddress), nodePort));
  }

  public static Client newClient() {
    return newClient(NODE_ADDRESS, NODE_PORT);
  }

}
