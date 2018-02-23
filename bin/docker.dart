import "dart:convert";
import "dart:io";

import "package:dslink/utils.dart";

import "package:dsbroker/broker.dart";

import "broker.dart" as CompatibilityMode;

Map<String, String> fullEnvironment = <String, String>{};

bool getBooleanEnv(String key, [bool defaultValue = false]) {
  String value = fullEnvironment[key];

  if (value == null || value.isEmpty) {
    return defaultValue;
  }

  if (const [
    "true",
    "1",
    "yes",
    "enabled",
    "enable",
    "on"
  ].contains(value.toLowerCase())) {
    return true;
  }

  if (const [
    "false",
    "0",
    "no",
    "disabled",
    "disable",
    "off"
  ].contains(value.toLowerCase())) {
    return false;
  }

  return defaultValue;
}

Map<String, Map<String, String>> getEnvConfigs(String prefix) {
  var fullPrefix = "BROKER_${prefix.toUpperCase()}_";

  var result = <String, Map<String, String>>{};

  for (String key in fullEnvironment.keys) {
    if (key.startsWith(fullPrefix)) {
      var section = key.substring(fullPrefix.length);
      if (!section.contains("_")) {
        result[section] = JSON.decode(fullEnvironment[key]);
      } else {
        var parts = section.split("_");
        var root = parts[0];
        var ckey = parts.skip(1).join("_");

        var cfg = result[root];
        if (cfg is! Map) {
          cfg = result[root] = <String, String>{};
        }

        cfg[ckey] = fullEnvironment[key];
      }
    }
  }

  return result;
}

int getIntegerEnv(String key, [int defaultValue = 0]) {
  String value = fullEnvironment[key];

  if (value == null || value.isEmpty) {
    return defaultValue;
  }

  var result = int.parse(value, onError: (_) => null);

  if (result == null) {
    return defaultValue;
  }

  return result;
}

Map<String, String> getDefaultEnvironment() {
  var env = <String, String>{};
  env["BROKER_HOST"] = "0.0.0.0";
  env["BROKER_PORT"] = "8080";
  env["BROKER_ALLOW_ALL_LINKS"] = "true";
  env["BROKER_ENABLE_QUARANTINE"] = "false";
  env["BROKER_LOG_LEVEL"] = "info";
  return env;
}

main(List<String> args) async {
  if (args.contains("--compatible")) {
    return await CompatibilityMode.main(args);
  }

  fullEnvironment.addAll(getDefaultEnvironment());
  fullEnvironment.addAll(Platform.environment);

  updateLogLevel(fullEnvironment["BROKER_LOG_LEVEL"]);

  var broker = new BrokerNodeProvider(
    enabledQuarantine: getBooleanEnv("BROKER_ENABLE_QUARANTINE"),
    acceptAllConns: getBooleanEnv("BROKER_ALLOW_ALL_LINKS", true),
    downstreamName: "downstream"
  );

  broker.loadHandler = (key) async {
    return {};
  };

  broker.saveHandler = (key, value) async {
  };

  broker.setConfigHandler = (key, value) {
    logger.warning(
        "Broker attempted to set '${key}', but this is"
            " not supported in a Docker configuration."
    );
  };

  new DsHttpServer.start(
    fullEnvironment["BROKER_HOST"],
    httpPort: getIntegerEnv("BROKER_PORT", 8080),
    nodeProvider: broker,
    linkManager: broker,
    sslContext: SecurityContext.defaultContext,
    shouldSaveFiles: false
  );

  await broker.loadAll();

  broker.upstream.onUpdate = (map) async {
    logger.warning(
        "Broker attempted to update upstreams, but this is"
            " not supported in a Docker configuration."
    );
  };

  var upstreams = getEnvConfigs("UPSTREAM");

  for (var upstreamName in upstreams.keys) {
    var cfg = upstreams[upstreamName];

    broker.upstream.addUpstreamConnection(
      upstreamName,
      cfg["URL"],
      cfg["OURS"] == null ? Platform.localHostname : cfg["OURS"],
      cfg["TOKEN"],
      cfg["GROUP"]
    );
  }

  var handleSignal = (ProcessSignal signal) {
    broker.disconnectAllLinks();
    exit(0);
  };

  ProcessSignal.SIGINT.watch().listen(handleSignal);

  if (!Platform.isWindows) {
    ProcessSignal.SIGTERM.watch().listen(handleSignal);
  }
}
