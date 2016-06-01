import "dart:async";
import "dart:convert";
import "dart:io";

import "package:dslink/client.dart";
import "package:dslink/utils.dart";
import "package:dslink/dslink.dart";

import "package:dsbroker/broker.dart";

BrokerNodeProvider broker;
DsHttpServer server;
LinkProvider link;
BrokerDiscoveryClient discovery;

const Map<String, String> VARS = const {
  "BROKER_PORT": "port",
  "BROKER_HOST": "host",
  "BROKER_BROADCAST": "broadcast",
  "BROKER_BROADCAST_URL": "broadcastUrl",
  "BROKER_DOWNSTREAM_NAME": "downstreamName"
};

Future<String> getNetworkAddress() async {
  List<NetworkInterface> interfaces = await NetworkInterface.list();
  if (interfaces == null || interfaces.isEmpty) {
    return null;
  }
  NetworkInterface interface = interfaces.first;
  List<InternetAddress> addresses = interface.addresses
      .where((it) => !it.isLinkLocal && !it.isLoopback)
      .toList();
  if (addresses.isEmpty) {
    return null;
  }
  return addresses.first.address;
}

main(List<String> _args) async {
  var args = new List<String>.from(_args);
  var configFile = new File("broker.json");
  var https = false;

  if (args.contains("--docker")) {
    args.remove("--docker");
    var config = {
      "host": "0.0.0.0",
      "port": 8081,
      "broadcast": true,
      "downstreamName": "downstream"
    };

    VARS.forEach((n, c) {
      if (Platform.environment.containsKey(n)) {
        var v = Platform.environment[n];
        if (v == "true" || v == "false") {
          v = v == "true";
        }

        var number = num.parse(v, (_) => null);

        if (number != null) {
          v = number;
        }

        config[c] = v;
      }
    });

    await configFile.writeAsString(JSON.encode(config));
  }

  if (!(await configFile.exists())) {
    await configFile.create(recursive: true);
    await configFile.writeAsString(defaultConfig);
  }

  var config = JSON.decode(await configFile.readAsString());

  dynamic getConfig(String key, [defaultValue]) {
    if (!config.containsKey(key)) {
      return defaultValue;
    }
    var value = config[key];

    if (value == null) {
      return defaultValue;
    }

    return value;
  }

  saveConfig() {
    var data = const JsonEncoder.withIndent("  ").convert(config);
    configFile.writeAsStringSync(data + '\n');
  }

  updateLogLevel(getConfig("logLevel", "info"));
  var downstreamName = getConfig("downstreamName", "downstream");
  broker = new BrokerNodeProvider(downstreamName: downstreamName);

  int httpsPort = getConfig("httpsPort", 8443);
  String sslCertificatePath = getConfig("sslCertificatePath", "");
  String sslKeyPath = getConfig("sslKeyPath", "");
  String sslCertificatePassword = getConfig("sslCertificatePassword", "");
  String sslKeyPassword = getConfig("sslKeyPassword", "");

  if (sslCertificatePassword.isEmpty) {
    sslCertificatePassword = null;
  }

  if (sslKeyPassword.isEmpty) {
    sslKeyPassword = null;
  }

  SecurityContext context = SecurityContext.defaultContext;

  if (httpsPort > 0 && sslCertificatePath.isNotEmpty && sslKeyPath.isNotEmpty) {
    context.useCertificateChain(
      sslCertificatePath,
      password: sslCertificatePassword
    );

    context.usePrivateKey(sslKeyPath, password: sslKeyPassword);
  }

  server = new DsHttpServer.start(
    getConfig("host", "0.0.0.0"),
    httpPort: getConfig("port", 8080),
    httpsPort: httpsPort,
    nodeProvider: broker,
    linkManager: broker,
    sslContext: context
  );

  https = getConfig("httpsPort", -1) != -1;

  if (getConfig("broadcast", false)) {
    var addr = await getNetworkAddress();
    var scheme = https ? "https" : "http";
    var port = https ? getConfig("httpsPort") : getConfig("port");
    var url = getConfig("broadcastUrl", "${scheme}://${addr}:${port}/conn");
    print("Starting Broadcast of Broker at ${url}");
    discovery = new BrokerDiscoveryClient();
    try {
      await discovery.init(true);
      discovery.requests.listen((BrokerDiscoverRequest request) {
        request.reply(url);
      });
    } catch (e) {
      print(
          "Warning: Failed to start broker broadcast service."
            "Are you running more than one broker on this machine?");
    }
  }

  await broker.loadAll();

  if (getConfig("upstream") != null) {
    Map<String, Map<String, dynamic>> upstream = getConfig("upstream", {});

    for (var name in upstream.keys) {
      var url = upstream[name]["url"];
      var ourName = upstream[name]["name"];
      var enabled = upstream[name]["enabled"];
      var group = upstream[name]["group"];
      broker.upstream.addUpstreamConnection(
        name,
        url,
        ourName,
        group,
        enabled
      );
    }
  }

  broker.upstream.onUpdate = (map) async {
    config["upstream"] = map;
    saveConfig();
  };

  broker.setConfigHandler = (String name, dynamic value) async {
    config[name] = value;
    saveConfig();
  };

  var handleSignal = (ProcessSignal signal) {
    broker.disconnectAllLinks();
    exit(0);
  };

  ProcessSignal.SIGINT.watch().listen(handleSignal);
  ProcessSignal.SIGTERM.watch().listen(handleSignal);
}

final String defaultConfig = const JsonEncoder.withIndent("  ").convert({
  "host": "0.0.0.0",
  "port": 8080,
  "httpsPort": 8443,
  "downstreamName": "downstream",
  "logLevel": "info",
  "quarantine": false,
  "allowAllLinks": true,
  "upstream": {},
  "sslCertificatePath": "",
  "sslKeyPath": "",
  "sslCertificatePassword": "",
  "sslKeyPassword": ""
});
