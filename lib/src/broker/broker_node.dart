part of dsbroker.broker;

/// Wrapper node for brokers
class BrokerNode extends LocalNodeImpl with BrokerNodePermission {
  final BrokerNodeProvider provider;

  BrokerNode(String path, this.provider) : super(path);

  @override
  void load(Map m) {
    super.load(m);
    if (m["?permissions"] is List) {
      loadPermission(m["?permissions"]);
    }
  }

  @override
  Map serialize(bool withChildren) {
    Map rslt = super.serialize(withChildren);
    List permissionData = this.serializePermission();
    if (permissionData != null) {
      rslt["?permissions"] = permissionData;
    }
    return rslt;
  }

  BrokerNodePermission getPermissionChild(String str) {
    if (children[str] is BrokerNodePermission) {
      return children[str] as BrokerNodePermission;
    }
    return null;
  }

  bool persist() {
    return false;
  }
}

/// nodes that automatic add itself to broker tree and always stay there
class BrokerStaticNode extends BrokerNode {
  BrokerStaticNode(String path, BrokerNodeProvider provider) : super(path, provider) {
    provider.setNode(path, this);
  }
}

/// Version node
class BrokerVersionNode extends BrokerStaticNode {
  BrokerVersionNode(String path, BrokerNodeProvider provider, String version) : super(path, provider) {
    configs[r"$name"] = "DSA Version";
    configs[r"$type"] = "string";
    updateValue(version);
  }
}

/// Start Time node
class StartTimeNode extends BrokerStaticNode {
  StartTimeNode(String path, BrokerNodeProvider provider) : super(path, provider) {
    configs[r"$name"] = "Start Time";
    configs[r"$type"] = "time";
    updateValue(ValueUpdate.getTs());
  }
}

/// Clear Conns node
class ClearConnsAction extends BrokerStaticNode {
  ClearConnsAction(String path, BrokerNodeProvider provider) : super(path, provider) {
    configs[r"$name"] = "Clear Conns";
    configs[r"$invokable"] = "config";
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {
    provider.clearConns();
    return response..close();
  }
}

class RootNode extends BrokerNode {
  RootNode(String path, BrokerNodeProvider provider) : super(path, provider) {
    configs[r"$is"] = "dsa/broker";

    if (provider != null) {
      configs[r"$uid"] = provider.uid;
    }
  }

  bool _loaded = false;

  void load(Map m) {
    if (_loaded) {
      throw "root node can not be initialized twice";
    }

    m.forEach((String key, value) {
      if (key.startsWith(r"$")) {
        configs[key] = value;
      } else if (key.startsWith("@")) {
        attributes[key] = value;
      } else if (value is Map) {
        BrokerNode node;
        if (value == "defs") {
          node = new BrokerHiddenNode("/$key", provider);
        } else {
          node = new BrokerNode("/$key", provider);
        }

        node.load(value);
        provider.nodes[node.path] = node;
        children[key] = node;
      }
    });
  }
}


class BrokerHiddenNode extends BrokerNode {
  BrokerHiddenNode(String path, BrokerNodeProvider provider) : super(path, provider) {
    configs[r"$hidden"] = true;
  }

  Map getSimpleMap() {
    Map rslt = {r"$hidden":true};
    if (configs.containsKey(r"$is")) {
      rslt[r"$is"] = configs[r"$is"];
    }
    if (configs.containsKey(r"$type")) {
      rslt[r"$type"] = configs[r"$type"];
    }
    if (configs.containsKey(r"$name")) {
      rslt[r"$name"] = configs[r"$name"];
    }
    if (configs.containsKey(r"$invokable")) {
      rslt[r"$invokable"] = configs[r"$invokable"];
    }
    if (configs.containsKey(r"$writable")) {
      rslt[r"$writable"] = configs[r"$writable"];
    }
    // TODO: add permission of current requester
    return rslt;
  }
}
