/// DSA Broker Implementation
library dsbroker.broker;

import "dart:async";
import "dart:io";
import "dart:convert";
import "dart:typed_data";

import "package:dslink/nodes.dart";

import "package:dslink/client.dart" show HttpClientLink, PrivateKey;
import "package:dslink/responder.dart";
import "package:dslink/requester.dart";
import "package:dslink/common.dart";
import "package:dslink/server.dart";
import "package:dslink/utils.dart";
import "package:dslink/src/http/websocket_conn.dart";
import "package:dslink/query.dart";
import "package:dslink/io.dart";
import "package:dslink/src/storage/simple_storage.dart";
import "package:uuid/uuid.dart";

part "src/broker/broker_node_provider.dart";
part "src/broker/broker_node.dart";
part "src/broker/remote_node.dart";
part "src/broker/remote_root_node.dart";
part "src/broker/remote_requester.dart";
part "src/broker/broker_permissions.dart";
part "src/broker/broker_alias.dart";
part "src/broker/user_node.dart";
part "src/broker/trace_node.dart";
part "src/broker/throughput.dart";
part "src/broker/data_nodes.dart";
part "src/broker/tokens.dart";
part "src/broker/responder.dart";
part "src/broker/stats.dart";
part "src/broker/quarantine.dart";
part "src/broker/upstream.dart";
part "src/broker/config_node.dart";
part "src/broker/icon.dart";

part "src/http/server_link.dart";
part "src/http/server.dart";

part "src/broker/permission_actions.dart";
part "src/broker/query_node.dart";
part "src/broker/broker_profiles.dart";

class BrokerGlobalConfig {
  static String BROKER_DIST = "dart";
}

Future<DsHttpServer> startBrokerServer(int port, {
  bool persist: true,
  BrokerNodeProvider broker,
  host,
  bool loadAllData: true
}) async {
  if (host == null) {
    host = InternetAddress.ANY_IP_V4;
  }
  if (broker == null) {
    broker = new BrokerNodeProvider(
      downstreamName: "downstream"
    );
  }
  broker.shouldSaveFiles = persist;
  var server = new DsHttpServer.start(
    host,
    httpPort: port,
    nodeProvider: broker,
    linkManager: broker,
    shouldSaveFiles: persist
  );

  if (loadAllData) {
    await broker.loadAll();
  }

  await server.onServerReady;
  return server;
}

PrivateKey loadBrokerPrivateKey({bool save: true}) {
  File keyFile = new File(".dslink.key");
  PrivateKey privateKey;
  String key;

  try {
    key = keyFile.readAsStringSync();
    privateKey = new PrivateKey.loadFromString(key);
  } catch (err) {}

  if (key == null || key.length != 131) {
    // 43 bytes d, 87 bytes Q, 1 space
    // generate the key
    if (DSRandom.instance.needsEntropy) {
      String macs;
      if (Platform.isWindows) {
        macs = Process.runSync("getmac", []).stdout.toString();
      } else {
        try {
          macs = Process.runSync("arp", ["-an"]).stdout.toString();
        } catch (e) {
          try {
            var envs = "";
            for (var i in Platform.environment.keys) {
              envs += "${i}=${Platform.environment[i]}\n";
            }
            macs = envs;
          } catch (e) {}
        }
      }
      // randomize the PRNG with the system mac (as well as timestamp)
      DSRandom.instance.addEntropy(macs);
    }
    privateKey = new PrivateKey.generateSync();
    key = privateKey.saveToString();
    if (save == true) {
      keyFile.writeAsStringSync(key);
    }
  }

  return privateKey;
}
