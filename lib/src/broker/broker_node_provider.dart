part of dsbroker.broker;

typedef void BrokerConfigSetHandler<T>(String name, T value);

class BrokerNodeProvider extends NodeProviderImpl implements ServerLinkManager {
  /// map that holds all nodes
  /// a node is not in parent node"s children when real data/connection doesn"t exist
  /// but instance is still there
  final Map<String, LocalNode> nodes =
    new Map<String, LocalNode>();

  /// connPath to connection
  final Map<String, RemoteLinkManager> conns =
    new Map<String, RemoteLinkManager>();

  BrokerPermissions permissions;

  IStorageManager storage;

  String downstreamName;
  /// name with 1 slash
  String downstreamNameS;
  /// name with 2 slash
  String downstreamNameSS;

  RootNode root;
  BrokerNode connsNode;
  BrokerDataRoot dataNode;
  BrokerNode usersNode;
  BrokerHiddenNode defsNode;
  BrokerNode sysNode;
  BrokerNode upstreamDataNode;
  BrokerNode quarantineNode;
  BrokerNode tokens;

  BrokerConfigSetHandler setConfigHandler;

  String uid;

  BrokerStatsController stats;

  Map rootStructure = {"users": {}, "sys": {"tokens": {},"quarantine":{}}, "upstream": {}};

  bool shouldSaveFiles = true;
  bool enabledQuarantine = false;
  bool enabledDataNodes = false;
  bool acceptAllConns = true;

  ThroughPutController throughput;
  BrokerTraceNode traceNode;
  UpstreamNode upstream;

  IValueStorageBucket attributeStorageBucket;

  TokenContext tokenContext;

  AuthorizeDSLinkAction approveDslinkAction;
  KickDSLinkAction kickDslinkAction;
  UpdateGroupAction updateGroupAction;
  List defaultPermission;

  BrokerNodeProvider({
    this.enabledQuarantine: false,
    this.acceptAllConns: true,
    this.defaultPermission,
    this.downstreamName: "conns",
    IStorageManager storage,
    this.enabledDataNodes: true
  }) {
    if (storage == null) {
      storage = new SimpleStorageManager("storage");
    }

    uid = generateToken();

    this.storage = storage;

    throughput = new ThroughPutController();
    permissions = new BrokerPermissions();
    traceNode = new BrokerTraceNode(this);
    tokenContext = new TokenContext(this);

    // initialize root nodes
    root = new RootNode("/", this);

    nodes["/"] = root;

    if (downstreamName == null ||
      downstreamName == "" ||
      rootStructure.containsKey(downstreamName) ||
      downstreamName.contains(Path.invalidNameChar)) {
      throw "invalid downstreamName";
    }
    downstreamNameS = "/$downstreamName";
    downstreamNameSS = "$downstreamNameS/";
    rootStructure[downstreamName] = {};

    root.load(rootStructure);

    defsNode = new BrokerHiddenNode("/defs", this);
    root.addChild("defs", defsNode);

    connsNode = nodes[downstreamNameS];
    connsNode.configs[r"$downstream"] = true;
    root.configs[r"$downstream"] = downstreamNameS;
    usersNode = nodes["/users"];
    dataNode = nodes["/data"];
    sysNode = nodes["/sys"];
    quarantineNode = nodes["/sys/quarantine"];
    upstreamDataNode = nodes["/upstream"];
    tokens = nodes["/sys/tokens"];
    new BrokerQueryNode("/sys/query", this);


    if (defaultPermission != null) {
      fixPermissionList(defaultPermission);
      // example: ["dgSuper", "config", "default", "write"]
      root.loadPermission(defaultPermission);
    }
    defsNode.loadPermission([["default", "read"]]);
    sysNode.loadPermission([[":config", "config"],["default", "none"]]);
    quarantineNode.loadPermission([[":config", "config"],["default", "none"]]);

    permissions.root = root;
  }

  void updateDefaultGroups(List list) {
    root.loadPermission(list);
    approveDslinkAction.updateGroups(list);
    upstream.createAction.updateGroups(list);
    updateGroupAction.updateGroups(list);
  }

  static void fixPermissionList(List plist) {
    Map builtinPermissions = {
      ":trustedLink":"config",
      ":user":"read",
      ":config":"config",
      ":write":"write",
      ":read":"read",
    };

    for (List l in plist) {
      builtinPermissions.forEach((String g, String p) {
        if (l[0] == g) {
          builtinPermissions[g] = null;
        }
      });
    }

    builtinPermissions.forEach((String g, String p) {
      if (p != null) {
        plist.insert(0, [g, p]);
      }
    });
  }

  loadAll() async {
    List<List<ISubscriptionNodeStorage>> storedData;
    if (storage != null) {
      storedData = await storage.loadSubscriptions();
    }
    await loadDef();
    registerInvokableProfile(_userNodeFunctions);
    registerInvokableProfile(_tokenNodeFunctions);
    initSys();
    await loadConns();
    await loadUserNodes();

    // tokens need to check if node still exists
    // load token after conns and userNodes are loaded
    await loadTokensNodes();

    if (enabledDataNodes) {
      await loadDataNodes();
      registerInvokableProfile(_dataNodeFunctions);
    }

    if (storage != null) {
      loadOverrideAttributes();
    }

    if (storedData != null) {
      for (List<ISubscriptionNodeStorage> nodeData in storedData) {
        if (nodeData.length > 0) {
          var nodeStorage = nodeData[0].storage;
          String path = nodeStorage.responderPath;
          if (path != null && conns.containsKey(path)) {
            conns[path].getResponder(this, null).initStorage(nodeStorage, nodeData);
          } else {
            nodeStorage.destroy();
          }
        }
      }
    }
  }

  void initSys() {
    new BrokerVersionNode("/sys/version", this, DSA_VERSION);
    new StartTimeNode("/sys/startTime", this);
    new BrokerDistNode("/sys/dist", this, BrokerGlobalConfig.BROKER_DIST);
    new ClearConnsAction("/sys/clearConns", this);
    new UpdatePermissionAction("/sys/updatePermissions", this);

    throughput.initNodes(this);
    upstream = new UpstreamNode("/sys/upstream", this);

    traceNode.init();

    stats = new BrokerStatsController(this);
    stats.init();

    approveDslinkAction = new AuthorizeDSLinkAction(
      "/sys/quarantine/authorize",
      this
    );

    if (defaultPermission != null) {
      approveDslinkAction.updateGroups(defaultPermission);
    }
    kickDslinkAction = new KickDSLinkAction("/sys/quarantine/deauthorize", this);
    updateGroupAction = new UpdateGroupAction("/sys/updateGroup", this);

    new UpdateDefaultPermission("/sys/updateDefaultPermission", this)
      ..updateData(defaultPermission);

    new AllowAllLinksNode("/sys/allowAllLinks", this);
    new EnableQuarantineNode("/sys/enableQuarantine", this);
  }

  /// load a fixed profile map
  loadDef() async {
    DefinitionNode profileNode = getOrCreateNode("/defs/profile", false);
    defsNode.children["profile"] = profileNode;
    defaultProfileMap.forEach((String name, Map m) {
      String path = "/defs/profile/$name";
      DefinitionNode node = getOrCreateNode(path, false);
      node.load(m);
      profileNode.children[name] = node;
    });

    brokerProfileMap.forEach((String name, Map m) {
      String path = "/defs/profile/$name";
      DefinitionNode node = getOrCreateNode(path, false);
      node.load(m);
      profileNode.children[name] = node;
    });

    File connsFile = new File("defs.json");
    try {
      String data = await connsFile.readAsString();
      Map m = DsJson.decode(data);
      m.forEach((String name, Map m) {
        String path = "/defs/$name";
        DefinitionNode node = getOrCreateNode(path, false);
        node.load(m);
        defsNode.children[name] = node;
      });
    } catch (err) {}
  }

  void registerInvokableProfile(Map m) {
    void register(Map m, String path) {
      m.forEach((String key, Object val) {
        if (val is Map) {
          register(val, "$path$key/");
        } else if (val is InvokeCallback) {
          (getOrCreateNode("$path$key", false) as DefinitionNode)
            .setInvokeCallback(val);
        }
      });
    }
    register(m, "/defs/profile/");
  }

  loadUserNodes() async {
    File connsFile = new File("usernodes.json");
    try {
      String data = await connsFile.readAsString();
      Map m = DsJson.decode(data);
      m.forEach((String name, Map m) {
        String path = "/users/$name";
        UserRootNode node = getOrCreateNode(path, false);
        node.loadPermission([[name, "config"], ["default", "none"]]);
        node.load(m);
        usersNode.children[name] = node;
      });
    } catch (err) {}
  }

  Future<Map> saveUsrNodes() async {
    Map m = {};
    usersNode.children.forEach((String name, LocalNodeImpl node) {
      m[name] = node.serialize(true);
    });
    File connsFile = new File("usernodes.json");
    if (shouldSaveFiles) {
      await connsFile.writeAsString(DsJson.encode(m));
    }
    return m;
  }

  loadConns() async {
    // loadConns from file
    File connsFile = new File("conns.json");
    try {
      String data = await connsFile.readAsString();
      Map m = DsJson.decode(data);
      List names = [];
      m.forEach((String name, Map m) {
        String path = "$downstreamNameSS$name";
        RemoteLinkRootNode node = getOrCreateNode(path, false);
        connsNode.children[name] = node;
        RemoteLinkManager conn = node._linkManager;
        conn.inTree = true;
        connsNode.updateList(name);

        node.load(m);
        if (node.configs[r"$$dsId"] is String) {
          _id2connPath[node.configs[r"$$dsId"]] = path;
          _connPath2id[path] = node.configs[r"$$dsId"];
        }
        names.add(name);
      });
      kickDslinkAction.updateNames(names);
    } catch (err) {}
  }

  loadOverrideAttributes() async {
    IValueStorageBucket storageBucket = storage.getOrCreateValueStorageBucket("attribute");
    attributeStorageBucket = storageBucket;
    logger.finest("loading proxy attributes");
    Map values = await storageBucket.load();
    values.forEach((key, val) {
      LocalNode node = this.getOrCreateNode(key, false);
      if (node is RemoteLinkNode && node._linkManager.inTree) {
        node.updateOverrideAttributes(val);
      } else {
        storageBucket.getValueStorage(key).destroy();
      }
    });
  }

  loadDataNodes() async {
    logger.finest("Loading Data Nodes");
    dataNode = new BrokerDataRoot("/data", this);
    if (storage != null) {
      dataNode.storageBucket = storage
        .getOrCreateValueStorageBucket("data");
    }
    dataNode.init();

    root.children["data"] = dataNode;
    nodes["/data"] = dataNode;

    File connsFile = new File("data.json");
    try {
      String data = await connsFile.readAsString();
      Map m = DsJson.decode(data);
      m.forEach((String name, Map m) {
        String path = "/data/$name";
        BrokerDataNode node = getOrCreateNode(path, true);
        node.load(m);
      });
    } catch (err) {}

    if (storage != null) {
       Map values = await dataNode.storageBucket.load();
       values.forEach((key, val) {
         if (nodes[key] is BrokerDataNode) {
           nodes[key].updateValue(val);
         } else {
           dataNode.storageBucket.getValueStorage(key).destroy();
         }
       });
    }
  }

  Future<Map> saveDataNodes() async {
    Map m = {};
    dataNode.children.forEach((String name, BrokerDataNode node) {
      m[name] = node.serialize(true);
    });
    File dataFile = new File("data.json");
    if (shouldSaveFiles) {
      await dataFile.writeAsString(DsJson.encode(m));
    }
    return m;
  }

  loadTokensNodes() async {
    File connsFile = new File("tokens.json");
    try {
      String data = await connsFile.readAsString();
      Map m = DsJson.decode(data);
      m.forEach((String name, Map m) {
        String path = "/sys/tokens/$name";
        TokenGroupNode tokens = new TokenGroupNode(path, this, name);
        tokens.load(m);
      });
    } catch (err) {
      String path = "/sys/tokens/root";
      TokenGroupNode tokens = new TokenGroupNode(path, this, "root");
      tokens.init();
    }
  }

  Future<Map> saveTokensNodes() async {
    Map m = {};
    tokens.children.forEach((String name, TokenGroupNode node) {
      m[name] = node.serialize(true);
    });
    File connsFile = new File("tokens.json");
    if (shouldSaveFiles) {
      await connsFile.writeAsString(DsJson.encode(m));
    }
    return m;
  }

  Future<Map> saveConns() async {
    Map m = {};
    List names = [];
    connsNode.children.forEach((String name, RemoteLinkNode node) {
      names.add(name);
      RemoteLinkManager manager = node._linkManager;
      m[name] = manager.rootNode.serialize(false);
    });
    File connsFile = new File("conns.json");
    if (shouldSaveFiles) {
      await connsFile.writeAsString(DsJson.encode(m));
    }
    kickDslinkAction.updateNames(names);
    updateGroupAction.updateNames(names);
    return m;
  }
  void updateQuarantineIds() {
    List qIds = [];
    quarantineNode.children.forEach((String key, BrokerNode node) {
      Object id = node.configs[r"$$dsId"];
      if (id is String) {
        qIds.add(id);
      }
    });
    approveDslinkAction.updateDsId(qIds);
  }
  // remove disconnected nodes from the conns node
  void clearConns() {
    List names = connsNode.children.keys.toList();
    for (String name in names) {
      RemoteLinkNode node = connsNode.children[name];
      RemoteLinkManager manager = node._linkManager;
      if (manager.disconnected != null) {
        String fullId = _connPath2id[manager.path];
        _connPath2id.remove(manager.path);
        _id2connPath.remove(fullId);
        connsNode.children.remove(name);
        manager.inTree = false;
        // remove server link if it"s not connected
        if (_links.containsKey(fullId)) {
          _links.remove(fullId).close();
        }
        connsNode.updateList(name);
      }
    }
    DsTimer.timerOnceBefore(saveConns, 300);
  }

  void clearUpstreamNodes() {
    List names = upstreamDataNode.children.keys.toList();
    for (String name in names) {
      var val = upstreamDataNode.children[name];
      if (val is! RemoteLinkNode) {
        continue;
      }

      RemoteLinkNode node = val;
      RemoteLinkManager manager = node._linkManager;
      if (manager.disconnected != null || !upstream.children.containsKey(name)) {
        String fullId = _connPath2id[manager.path];
        _connPath2id.remove(manager.path);
        _id2connPath.remove(fullId);
        upstreamDataNode.children.remove(name);
        manager.inTree = false;
        // remove server link if it"s not connected
        if (_links.containsKey(fullId)) {
          _links.remove(fullId);
        }
        upstreamDataNode.updateList(name);
      }
    }
    DsTimer.timerOnceBefore(saveConns, 300);
  }

  /// add a node to the tree
  void setNode(String path, LocalNode newNode) {
    LocalNode node = nodes[path];
    if (node != null) {
      logger.severe(
        "error, BrokerNodeProvider.setNode same node can not be set twice");
      return;
    }

    Path p = new Path(path);
    LocalNode parentNode = nodes[p.parentPath];
    if (parentNode == null) {
      logger.severe("error, BrokerNodeProvider.setNode parentNode is null");
      return;
    }

    nodes[path] = newNode;
    parentNode.addChild(p.name, newNode);
  }

  LocalNode getNode(String path) {
    return nodes[path];
  }

  BrokerDataNode _getOrCreateDataNode(String path, [bool addToTree = true]) {
    BrokerDataNode node = nodes[path];
    if (node == null) {
      node = new BrokerDataNode(path, this);
      node.configs[r"$type"] = "dynamic";
      nodes[path] = node;
    }

    if (addToTree && node.parent == null) {
       int pos = path.lastIndexOf("/");
       String parentPath = path.substring(0,pos);
       String name = path.substring(pos + 1);

       BrokerDataNode parentNode = _getOrCreateDataNode(parentPath, true);
       parentNode.children[name] = node;
       node.parent = parentNode;
       parentNode.updateList(name);

     }
     return node;
  }

  LocalNode getOrCreateNode(String path, [bool addToTree = true]) {
    if (path.startsWith("/data/")) {
      return _getOrCreateDataNode(path, addToTree);
    }

//    if (addToTree) {
//      print("getOrCreateNode, addToTree = true, not supported");
//    }

    LocalNode node = nodes[path];
    if (node != null) {
      return node;
    }
    if (path.startsWith("/users/")) {
      List paths = path.split("/");
      String username = path.split("/")[2];
      if (paths.length == 3) {
        node = new UserRootNode(path, username, this);
      } else {
        int pos = path.indexOf("/#");
        if (pos < 0) {
          node = new UserNode(path, this, username);
        } else {
          String connPath;
          int pos2 = path.indexOf("/", pos + 1);
          if (pos2 < 0) {
            connPath = path;
          } else {
            connPath = path.substring(0, pos2);
          }
          RemoteLinkManager conn = conns[connPath];
          if (conn == null) {
            // TODO conn = new RemoteLinkManager("$downstreamNameSS$connName", connRootNodeData);
            conn = new RemoteLinkManager(this, connPath, this);
            conns[connPath] = conn;
            nodes[connPath] = conn.rootNode;
            conn.rootNode.parentNode =
              getOrCreateNode(path.substring(0, pos), false);
          }
          node = conn.getOrCreateNode(path, false);
        }
      }
    } else if (path.startsWith(downstreamNameSS)) {
      String connName = path.split("/")[2];
      String connPath = "$downstreamNameSS$connName";
      RemoteLinkManager conn = conns[connPath];
      if (conn == null) {
        // TODO conn = new RemoteLinkManager("$downstreamNameSS$connName", connRootNodeData);
        conn = new RemoteLinkManager(this, connPath, this);
        conns[connPath] = conn;
        nodes[connPath] = conn.rootNode;
        conn.rootNode.parentNode = connsNode;
//        if (addToTree) {
//          connsNode.children[connName] = conn.rootNode;
//          conn.inTree = true;
//          connsNode.updateList(connName);
//        }
      }
      node = conn.getOrCreateNode(path, false);
    } else if (path.startsWith("/upstream/")) {
      String upstreamName = path.split("/")[2];
      String connPath = "/upstream/${upstreamName}";
      RemoteLinkManager conn = conns[connPath];
      if (conn == null) {
        conn = new RemoteLinkManager(this, connPath, this);
        conns[connPath] = conn;
        nodes[connPath] = conn.rootNode;

        conn.rootNode.parentNode = upstreamDataNode;
//        if (addToTree) {
//          upstreamDataNode.children[upstreamName] = conn.rootNode;
//          conn.inTree = true;
//          upstreamDataNode.updateList(upstreamName);
//        }
      }
      node = conn.getOrCreateNode(path, false);
    } else if (path.startsWith("/sys/quarantine/")) {
      List paths = path.split("/");
      if (paths.length > 3) {
        String connName = paths[3];
        String connPath = "/sys/quarantine/$connName";
        RemoteLinkManager conn = conns[connPath];
        if (conn == null) {
          // TODO conn = new RemoteLinkManager("$downstreamNameSS$connName", connRootNodeData);
          conn = new RemoteLinkManager(this, connPath, this);
          conns[connPath] = conn;
          nodes[connPath] = conn.rootNode;
          conn.rootNode.parentNode = quarantineNode;
          DsTimer.timerOnceBefore(updateQuarantineIds, 300);
        }
        node = conn.getOrCreateNode(path, false);
      } else {
        node = new BrokerNode(path, this);
      }
    } else if (path.startsWith("/defs/")) {
      //if (!_defsLoaded) {
      node = new DefinitionNode(path, this);
      //}
    } else {
      // TODO handle invalid node instead of allow everything
      node = new BrokerNode(path, this);
    }
    if (node != null) {
      nodes[path] = node;
    }
    return node;
  }

  bool clearNode(BrokerNode node) {
    // TODO, keep it in memory if there are pending subscription
    // and remove it when subscription ends
    if (nodes[node.path] == node) {
      nodes.remove(node);
    }
    return true;
  }

  /// dsId to server links
  final Map<String, BaseLink> _links = new Map<String, BaseLink>();
  final Map<String, String> _id2connPath = new Map<String, String>();
  final Map<String, String> _connPath2id = new Map<String, String>();

  Map<String, String> get id2connPath => _id2connPath;
  Map<String, BaseLink> get links => _links;
  Map<String, String> get connPath2id => _connPath2id;

  RemoteLinkManager getConnById(String id) {
    if (_id2connPath.containsKey(id)) {
      return conns[_id2connPath[id]];
    }
    return null;
  }

  RemoteLinkManager getConnPath(String path) {
    return conns[path];
  }

  String makeConnPath(String fullId, [bool allowed = false]) {
    if (_id2connPath.containsKey(fullId)) {
      return _id2connPath[fullId];
      // TODO is it possible same link get added twice?
    }

    if (fullId.startsWith("@upstream@")) {
      String connName = fullId.substring(10);
      String connPath = "/upstream/$connName";
      _connPath2id[connPath] = fullId;
      _id2connPath[fullId] = connPath;
      return connPath;
    }

    if (fullId.length < 43) {
      // user link
      String connPath = "$downstreamNameSS$fullId";
      int count = 0;
      // find a connName for it
      while (_connPath2id.containsKey(connPath)) {
        connPath = "$downstreamNameSS$fullId-${count++}";
      }
      _connPath2id[connPath] = fullId;
      _id2connPath[fullId] = connPath;
      return connPath;
    } else if (acceptAllConns || allowed) {
      // device link
      String connPath;
      String folderPath = downstreamNameSS;

      String dsId = fullId;
//      if (fullId.contains(":")) {
//        // uname:dsId
//        List<String> u_id = fullId.split(":");
//        folderPath = "/sys/quarantine/${u_id[0]}/";
//        dsId = u_id[1];
//      }

      // find a connName for it, keep append characters until find a new name
      int i = 43;
      if (dsId.length == 43) i = 42;
      for (; i >= 0; --i) {
        connPath = "$folderPath${dsId.substring(0, dsId.length - i)}";
        if (i == 43 && connPath.length > 8 && connPath.endsWith("-")) {
          // remove the last - in the name;
          connPath = connPath.substring(0, connPath.length - 1);
        }

        if (!_connPath2id.containsKey(connPath)) {
          _connPath2id[connPath] = fullId;
          _id2connPath[fullId] = connPath;
          break;
        }
      }

      DsTimer.timerOnceBefore(saveConns, 300);
      return connPath;
    } else if (enabledQuarantine) {
      String connPath;
      String folderPath = "/sys/quarantine/";
      connPath = "$folderPath$fullId";
      if (!_connPath2id.containsKey(connPath)) {
        _connPath2id[connPath] = fullId;
        _id2connPath[fullId] = connPath;
      }
      return connPath;
    } else {
      return null;
    }
  }

  String getLinkPath(String fullId, String token) {
    if (_id2connPath.containsKey(fullId)) {
      return _id2connPath[fullId];
    }

    if (token != null && token != "") {
      TokenNode tokenNode = tokenContext.findTokenNode(token, fullId);
      if (tokenNode != null) {
        BrokerNode target = tokenNode.getTargetNode();

        String connPath;

        String folderPath = "${target.path}/";

        String dsId = fullId;


        // find a connName for it, keep append characters until find a new name
        int i = 43;
        if (dsId.length == 43) i = 42;
        for (; i >= 0; --i) {
          connPath = "$folderPath${dsId.substring(0, dsId.length - i)}";
          if (i == 43 && connPath.length > 8 && connPath.endsWith("-")) {
            // remove the last - in the name;
            connPath = connPath.substring(0, connPath.length - 1);
          }
          if (!_connPath2id.containsKey(connPath)) {
            _connPath2id[connPath] = fullId;
            _id2connPath[fullId] = connPath;
            break;
          }
        }
        if (tokenNode.useToken(connPath)) {
          Node node = getOrCreateNode(connPath, false);
          node.configs[r"$$token"] = tokenNode.id;
        }
        if (tokenNode.group is String) {
          Node node = getOrCreateNode(connPath, false);
          node.configs[r"$$group"] = tokenNode.group;
        }
        DsTimer.timerOnceBefore(saveConns, 300);
        return connPath;
      }
    }

    // fall back to normal path searching when it fails
    return makeConnPath(fullId);
  }

  void prepareUpstreamLink(String name) {
    String connPath = "/upstream/$name";
    String upStreamId = "@upstream@$name";
    _connPath2id[connPath] = upStreamId;
    _id2connPath[upStreamId] = connPath;
  }

  RemoteLinkManager addUpstreamLink(ClientLink link, String name) {
    String upStreamId = "@upstream@$name";
    RemoteLinkManager conn;
    // TODO update children list of /$downstreamNameS node
    if (_links.containsKey(upStreamId)) {
      // TODO is it possible same link get added twice?
      return null;
    } else {
      _links[upStreamId] = link;

      String connPath = "/upstream/$name";
      _connPath2id[connPath] = upStreamId;
      _id2connPath[upStreamId] = connPath;
      RemoteLinkNode node = getOrCreateNode(connPath, false);
      upstreamDataNode.children[name] = node;
      upstreamDataNode.updateList(name);

      conn = node._linkManager;
      conn.inTree = true;

      logger.info("Link connected at ${connPath}");
    }

    if (!conn.inTree) {
      List paths = conn.path.split("/");
      String connName = paths.removeLast();
      BrokerNode parentNode = getOrCreateNode(paths.join("/"), false);
      parentNode.children[connName] = conn.rootNode;
      conn.rootNode.parentNode = parentNode;
      conn.inTree = true;
      parentNode.updateList(connName);
      DsTimer.timerOnceBefore(saveConns, 300);
    }
    return conn;
  }

  bool addLink(ServerLink link) {
    String str = link.dsId;
    if (link.session != "" && link.session != null) {
      str = "$str ${link.session}";
    }

    String connPath;
    // TODO update children list of /$downstreamNameS node
    if (_links.containsKey(str)) {
      // TODO is it possible same link get added twice?
    } else {
      if (str.length >= 43 && (link.session == null || link.session == "")) {
        // don"t create node for requester node with session
        connPath = makeConnPath(str);

        if (connPath != null) {
          LocalNode localNode = getOrCreateNode(connPath, false)
            ..configs[r"$$dsId"] = str;

          if (localNode is RemoteLinkRootNode) {
            localNode._linkManager.disconnected = null;
          }

          logger.info("Link connected at ${connPath}");
        } else {
          return false;
        }
      }
      _links[str] = link;
    }
    return true;
  }

  ServerLink getLinkAndConnectNode(String dsId, {String sessionId: ""}) {
    if (sessionId == null) sessionId = "";
    String str = dsId;
    if (sessionId != null && sessionId != "") {
      // user link
      str = "$dsId ${sessionId}";
    } else if (_links[str] != null) {
      // add link to tree when it"s not user link
      String connPath = makeConnPath(str);

      if (connPath == null) {
        // when link is not allowed, makeConnPath() returns null
        return null;
      }
      RemoteLinkNode node = getOrCreateNode(connPath, false);
      RemoteLinkManager conn = node._linkManager;
      if (!conn.inTree) {
        List paths = conn.path.split("/");
        String connName = paths.removeLast();
        BrokerNode parentNode = getOrCreateNode(paths.join("/"), false);
        parentNode.children[connName] = conn.rootNode;
        conn.rootNode.parentNode = parentNode;
        conn.inTree = true;
        parentNode.updateList(connName);
        if (conn.path.startsWith(downstreamNameSS)) {
          DsTimer.timerOnceBefore(saveConns, 300);
        } else if (conn.path.startsWith("/sys/quarantine/")) {
          DsTimer.timerOnceBefore(updateQuarantineIds, 300);
        }
      }
    }
    return _links[str];
  }

  void onLinkDisconnected(ServerLink link) {
    if (_links[link.dsId] == link) {
      String connPath = _id2connPath[link.dsId];
      if (connPath != null && connPath.startsWith("/sys/quarantine/")) {
        _connPath2id.remove(connPath);
        if (_id2connPath[link.dsId] == connPath) {
          // it"s also possible that the path is already moved to downstream
          // in that case, don"t remove, wait for it to connect
          _id2connPath.remove(link.dsId);
        }

        quarantineNode.children.remove(link.dsId);
        conns[connPath].inTree = false;

        // remove server link if it"s not connected
//        _links.remove(link.dsId);
        quarantineNode.updateList(link.dsId);
        DsTimer.timerOnceBefore(updateQuarantineIds, 300);
        _links.remove(link.dsId);
      }
    }
  }

  void removeLink(BaseLink link, String id, {bool force: false}) {
    if (_links[id] == link || force) {
      // TODO: any extra work needed in responder or requester?
      // link.responder.destroy();
      // link.requester.destroy();


      if (link is ServerLink) {
        // check if it"s a quaratine link
        // run this before disconnect event really happens
        onLinkDisconnected(link);
      }
      link.close();
      _links.remove(id);

      if (link is HttpServerLink && link.session != "" && link.session != null) {
        // fully destroy user link
        String connPath = _id2connPath[link.dsId];
        if (connPath != null) {
          RemoteLinkManager manager = conns[connPath];
          if (manager != null) {
            if (manager.responders.containsKey(link.session)) {
              manager.responders.remove(link.session);
            }

            if (link.wsconnection != null) {
              link.wsconnection = null;
            }

            if (link.connection != null) {
              link.connection = null;
            }
          }
        }

      }
    }
  }

  void remoteLinkByPath(String path) {
    Node node = nodes[path];
    if (node is RemoteLinkRootNode) {

      RemoteLinkManager manager = node._linkManager;

      String dsId = _connPath2id[path];
      if (dsId != null) {
        BaseLink link = _links[dsId];
        _connPath2id.remove(path);
        _id2connPath.remove(dsId);
        link.close();
        _links.remove(dsId);
        DsTimer.timerOnceBefore(saveConns, 300);
      }

      String name = node.path.split("/").last;
      connsNode.children.remove(name);
      manager.inTree = false;
      connsNode.updateList(name);
    }
  }

  void updateLinkData(String dsId, Map m) {
    if (_id2connPath.containsKey(dsId)) {
      var node = getOrCreateNode(_id2connPath[dsId], false);
      node.configs[r"$linkData"] = m;
    }
  }

  Requester getRequester(String dsId) {
    String connPath = makeConnPath(dsId);
    if (connPath == null) return null;

    if (conns.containsKey(connPath)) {
      return conns[connPath].requester;
    }

    /// create the RemoteLinkManager
    RemoteLinkNode node = getOrCreateNode(connPath, false);
    return node._linkManager.requester;
  }

  Responder getResponder(String dsId, NodeProvider nodeProvider,
    [String sessionId = "", bool trusted = false]) {
    String connPath = makeConnPath(dsId);
    if (connPath == null) return null;
    RemoteLinkNode node = getOrCreateNode(connPath, false);
    Responder rslt = node._linkManager.getResponder(nodeProvider, dsId, sessionId);
    if (connPath.startsWith("/sys/quarantine/")) {
      rslt.disabled = true;
      DsTimer.timerOnceBefore(updateQuarantineIds, 300);
    } else if (node.configs[r"$$group"] is String) {
      List groups = (node.configs[r"$$group"] as String).split(",");
      rslt.updateGroups(groups);
    } else if (trusted) {
      rslt.updateGroups([":trustedLink"]);
    }
    if (storage != null && sessionId == "" && rslt.storage == null) {
      rslt.storage = storage.getOrCreateSubscriptionStorage(connPath);
    }
    return rslt;
  }

  Responder createResponder(String dsId, String session) {
    return new BrokerResponder(this, dsId);
  }

  void updateConfigValue(String name, dynamic value) {
    if (setConfigHandler != null) {
      setConfigHandler(name, value);
    }
  }
}
