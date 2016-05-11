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
  IValueStorageBucket bucket;
  IValueStorage uidStorage;

  RootNode(String path, BrokerNodeProvider provider) : super(path, provider) {
    configs[r"$is"] = "dsa/broker";
    bucket = provider.storage.getOrCreateValueStorageBucket("ids");
    uidStorage = bucket.getValueStorage("broker");
    uidStorage.getValueAsync().then((id) {
      if (id == null) {
        id = generateToken();
        uidStorage.setValue(id);
      }
      configs[r"$uid"] = id;
    });
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

class UpstreamNode extends BrokerStaticNode {
  
  CreateUpstreamBrokerNode crateActoin;
  UpstreamNode(String path, BrokerNodeProvider provider)
  : super(path, provider) {
    new Future(() {
      crateActoin = new CreateUpstreamBrokerNode(
          "/sys/upstream/add_connection", provider);
      provider.setNode("/sys/upstream/add_connection", crateActoin);
      crateActoin.updateGroups(provider.defaultPermission);
    });
  }

  void addUpstreamConnection(
    String name,
    String url,
    String ourName,
    String token,
    String group,
    [bool enabled = true]) {
    if (enabled == null) {
      enabled = true;
    }

    var node = new UpstreamBrokerNode(
      "/sys/upstream/${name}",
      name,
      url,
      ourName,
      token,
      group,
      provider
    );
    provider.setNode("/sys/upstream/${name}", node);
    (provider.getOrCreateNode("/sys/upstream", false) as BrokerNode)
      .updateList(name);
    node.enabled = enabled;
    node.start();
  }

  void moveUpstreamConnection(String name, String newName) {
    BrokerNode node = provider.getOrCreateNode("/sys/upstream/${name}", false);

    if (node is UpstreamBrokerNode) {
      bool enabled = node.enabled;
      removeUpstreamConnection(name);
      addUpstreamConnection(newName, node.url, node.ourName, node.token, node.group, enabled);
    }
  }

  void removeUpstreamConnection(String name) {
    LocalNode node = provider.getOrCreateNode("/sys/upstream/${name}", false);
    if (node is UpstreamBrokerNode) {
      node.stop();
      node.toBeRemoved = true;
      children.remove(name);
      var rp = "/sys/upstream/${name}/";
      provider.nodes.remove(rp.substring(0, rp.length - 1));
      List<String> toRemove = provider.nodes
        .keys
        .where((x) => x.startsWith(rp))
        .toList();
      toRemove.forEach(provider.nodes.remove);
      updateList(name);
      provider.clearUpstreamNodes();
    }
  }

  void loadConfigMap(Map x) {
    for (var k in x.keys) {
      var m = x[k];
      addUpstreamConnection(k, m["url"], m["name"], m["token"], m["group"],m["enabled"]);
    }
  }

  Map getConfigMap() {
    List<UpstreamBrokerNode> ubns = provider.nodes.keys.where((x) {
      try {
        if (x.startsWith("/sys/upstream/") &&
        x != "/sys/upstream/add_connection") {
          return x.codeUnits.where((l) => l == "/".codeUnitAt(0)).length == 3;
        } else {
          return false;
        }
      } catch (e) {
        return false;
      }
    }).map((x) => provider.getOrCreateNode(x, false))
      .where((x) => x != null)
      .where((x) => x is UpstreamBrokerNode)
      .where((UpstreamBrokerNode x) => !x.toBeRemoved)
      .toList();

    var map = {};

    ubns.forEach((x) {
      map[x.name] = {
        "name": x.ourName,
        "url": x.url,
        "enabled": x.enabled,
        "token": x.token,
        "group": x.group
      };
    });

    return map;
  }

  void update() {
    if (onUpdate != null) {
      onUpdate(getConfigMap());
    }
  }

  Function onUpdate;
}

class CreateUpstreamBrokerNode extends BrokerNode {
  List params;
  CreateUpstreamBrokerNode(String path, BrokerNodeProvider provider)
  : super(path, provider) {
    configs[r"$name"] = "Add Upstream Connection";
    configs[r"$invokable"] = "write";
    params = [
      {
        "name": "Name",
        "type": "string",
        "description": "Upstream Broker Name",
        "placeholder": "UpstreamBroker"
      },
      {
        "name": "Url",
        "type": "string",
        "description": "Url to the Upstream Broker",
        "placeholder": "http://upstream.broker.com/conn"
      },
      {
        "name": "Broker Name",
        "type": "string",
        "description":"The name of the link when connected to the Upstream Broker",
        "placeholder": "ThisBroker"
      },
      {
        "name": "Token",
        "type": "string",
        "description": "Broker Token (if needed)",
        "placeholder": "OptionalAuthToken"
      },
      {
        "name": "Group",
        "type": "string"
      }
    ];
    configs[r"$params"] = params;
    configs[r"$result"] = "values";
  }

  void updateGroups(List defaultPermission) {
    if (defaultPermission == null) {
      return;
    }
    List groups = [];
    for (List p in defaultPermission) {
      groups.add(p[0]);
    }
    groups.sort();
    params[4] = {
      "name": "Group",
      "type": "string",
      "editor": "enum[${groups.join(',')}]"
    };
    updateList(r'$params');
  }
  
  @override
  InvokeResponse invoke(
      Map params, Responder responder, InvokeResponse response, Node parentNode,
      [int maxPermission = Permission.CONFIG]) {
    var name = params["Name"];
    var ourName = params["Broker Name"];
    var url = params["Url"];
    var token = params["Token"];
    var group = params["Group"];
    UpstreamNode b = provider.getOrCreateNode("/sys/upstream", false) as UpstreamNode;
    b.addUpstreamConnection(name, url, ourName, token, group);
    provider.upstream.update();
    return response..close();
  }
}

class DeleteUpstreamConnectionNode extends BrokerNode {
  final String name;

  DeleteUpstreamConnectionNode(
      String path, this.name, BrokerNodeProvider provider)
  : super(path, provider) {
    configs[r"$name"] = "Remove";
    configs[r"$invokable"] = "write";
    configs[r"$result"] = "values";
  }

  @override
  InvokeResponse invoke(
      Map params, Responder responder, InvokeResponse response, Node parentNode,
      [int maxPermission = Permission.CONFIG]) {
    var b = provider.getOrCreateNode("/sys/upstream", false) as UpstreamNode;
    b.removeUpstreamConnection(name);
    provider.upstream.update();
    return response..close();
  }
}

class UpstreamUrlNode extends BrokerNode {
  UpstreamUrlNode(String path, BrokerNodeProvider provider)
      : super(path, provider);

  Response setValue(Object value, Responder responder, Response response,
      [int maxPermission = Permission.CONFIG]) {
    if (value != null && value.toString().length > 0) {
      var p = new Path(path).parentPath;
      UpstreamBrokerNode un = provider.getOrCreateNode(p, false);

      un.provider.removeLink(un.link, "@upstream@${un.name}", force: true);
      un.stop();

      un.url = value.toString();
      un.enabled = true;
      un.start();

      provider.upstream.update();
      return super.setValue(value, responder, response, maxPermission);
    }

    return response..close();
  }
}

class UpstreamNameNode extends BrokerNode {
  UpstreamNameNode(String path, BrokerNodeProvider provider)
      : super(path, provider);

  Response setValue(Object value, Responder responder, Response response,
      [int maxPermission = Permission.CONFIG]) {
    if (value != null && value.toString().length > 0 &&
      provider.getNode("/sys/upstream/$value") == null) {
      var p = new Path(path).parentPath;
      UpstreamBrokerNode un = provider.getOrCreateNode(p, false);

      var b = provider.getOrCreateNode("/sys/upstream", false) as UpstreamNode;
      b.moveUpstreamConnection(un.name, value.toString());
    }

    provider.upstream.update();

    return response..close();
  }
}

class UpstreamTokenNode extends BrokerNode {
  UpstreamTokenNode(String path, BrokerNodeProvider provider)
    : super(path, provider);

  Response setValue(Object value, Responder responder, Response response,
    [int maxPermission = Permission.CONFIG]) {
    if (value != null && value.toString().length > 0) {
      var p = new Path(path).parentPath;
      UpstreamBrokerNode un = provider.getOrCreateNode(p, false);

      un.provider.removeLink(un.link, "@upstream@${un.name}", force: true);
      un.stop();

      un.token = value.toString();
      un.enabled = true;
      un.start();

      provider.upstream.update();
      return super.setValue(value, responder, response, maxPermission);
    }

    return response..close();
  }
}

class UpstreamGroupNode extends BrokerNode {
  UpstreamGroupNode(String path, BrokerNodeProvider provider)
    : super(path, provider);

  Response setValue(Object value, Responder responder, Response response,
    [int maxPermission = Permission.CONFIG]) {
    if (value != null && value.toString().length > 0) {
      var p = new Path(path).parentPath;
      UpstreamBrokerNode un = provider.getOrCreateNode(p, false);

      un.provider.removeLink(un.link, "@upstream@${un.name}", force: true);
      un.stop();

      un.group = value.toString();
      un.enabled = true;
      un.start();

      provider.upstream.update();
      return super.setValue(value, responder, response, maxPermission);
    }

    return response..close();
  }
}

class UpstreamEnabledNode extends BrokerNode {
  UpstreamEnabledNode(String path, BrokerNodeProvider provider)
  : super(path, provider);

  Response setValue(Object value, Responder responder, Response response,
      [int maxPermission = Permission.CONFIG]) {
    var p = new Path(path).parentPath;
    UpstreamBrokerNode un = provider.getOrCreateNode(p, false);

    if (value && un.enabled == false) {
      un.enabled = true;
      un.start();
    } else {
      un.enabled = false;
      un.stop();
    }

    provider.upstream.update();

    return super.setValue(value, responder, response, maxPermission);
  }
}

class UpstreamBrokerNode extends BrokerNode {
  String name;
  String url;
  String token;
  String group;

  final String ourName;

  UpstreamEnabledNode enabledNode;
  UpstreamUrlNode urlNode;
  UpstreamNameNode nameNode;
  UpstreamTokenNode tokenNode;
  UpstreamGroupNode groupNode;
  BrokerNode brokerNameNode;
  bool enabled = false;

  bool toBeRemoved = false;

  HttpClientLink link;

  UpstreamBrokerNode(String path, this.name, this.url, this.ourName,
                     this.token, this.group, BrokerNodeProvider provider)
  : super(path, provider) {
    enabledNode = new UpstreamEnabledNode("/sys/upstream/${name}/enabled", provider);
    enabledNode.configs[r"$type"] = "bool";
    enabledNode.configs[r"$writable"] = "write";
    enabledNode.updateValue(enabled);

    urlNode = new UpstreamUrlNode("/sys/upstream/${name}/url", provider);
    urlNode.configs[r"$type"] = "string";
    urlNode.configs[r"$writable"] = "write";
    urlNode.updateValue(url);

    nameNode = new UpstreamNameNode("/sys/upstream/${name}/name", provider);
    nameNode.configs[r"$type"] = "string";
    nameNode.configs[r"$writable"] = "write";
    nameNode.updateValue(name);

    brokerNameNode = new BrokerNode("/sys/upstream/${name}/brokerName", provider);
    brokerNameNode.configs[r"$type"] = "string";
    brokerNameNode.updateValue(ourName);

    tokenNode = new UpstreamTokenNode("/sys/upstream/${name}/token", provider);
    tokenNode.configs[r"$type"] = "string";
    tokenNode.configs[r"$writable"] = "write";
    tokenNode.updateValue(token);

    groupNode = new UpstreamGroupNode("/sys/upstream/${name}/group", provider);
    groupNode.configs[r"$type"] = "string";
    groupNode.configs[r"$writable"] = "write";
    groupNode.updateValue(group);
       
    new Future(() {
      var drn = new DeleteUpstreamConnectionNode(
          "/sys/upstream/${name}/delete", name, provider);
      provider.setNode("/sys/upstream/${name}/delete", drn);
      provider.setNode(enabledNode.path, enabledNode);
      provider.setNode(urlNode.path, urlNode);
      provider.setNode(nameNode.path, nameNode);
      provider.setNode(brokerNameNode.path, brokerNameNode);
      provider.setNode(tokenNode.path, tokenNode);
      provider.setNode(groupNode.path, groupNode);
      
      addChild("delete", drn);
      addChild("enabled", enabledNode);
      addChild("url", urlNode);
      addChild("name", nameNode);
      addChild("brokerName", brokerNameNode);
      addChild("token", tokenNode);
      addChild("group", groupNode);
    });
  }

  void start() {
    if (!enabled) {
      return;
    }

    BrokerNodeProvider p = provider;
    String upstreamId = "@upstream@$name";
    Requester overrideRequester = provider.getRequester(upstreamId);
    Responder overrideResponder = provider.getResponder(upstreamId, provider);
    PrivateKey pkey = loadBrokerPrivateKey();
    link = new HttpClientLink(
      url,
      "${ourName}-",
      pkey,
      nodeProvider: p,
      isRequester: true,
      isResponder: true,
      overrideRequester: overrideRequester,
      overrideResponder: overrideResponder,
      token: (
        token != null && token.isNotEmpty
      ) ? token : null
    );

    link.logName = "Upstream at /upstream/${name}";
    link.connect();

    RemoteLinkManager linkManager = p.addUpstreamLink(link, name);
    if (linkManager == null) {
      p.removeLink(link, "@upstream@$name", force: true);
      linkManager = p.addUpstreamLink(link, name);
    }
    linkManager.rootNode.configs[r'$$group'] = group;
    
    enabledNode.updateValue(true);
    enabled = true;
    link.onRequesterReady.then((Requester requester) {
      if (link.remotePath != null) {
        linkManager.rootNode.configs[r"$remotePath"] = link.remotePath;
        linkManager.rootNode.updateList(r"$remotePath");
      }
    });
  }

  void stop() {
    if (link == null) {
      return;
    }

    link.close();
    BrokerNodeProvider p = provider;

    p.removeLink(link, "@upstream@$name", force: true);
    enabledNode.updateValue(false);
    enabled = false;
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
