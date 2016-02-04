part of dsbroker.broker;

class RemoteLinkManager implements NodeProvider, RemoteNodeCache {
  final Map<String, RemoteLinkNode> nodes = new Map<String, RemoteLinkNode>();
  Requester requester;
  final String path;
  final BrokerNodeProvider broker;
  RemoteLinkRootNode rootNode;

  bool inTree = false;

  Iterable<String> get cachedNodePaths => nodes.keys;

  String disconnected = ValueUpdate.getTs();

  RemoteLinkManager(this.broker, this.path, NodeProviderImpl brokerProvider, [Map rootNodeData]) {
    requester = new RemoteRequester(this, path);
    rootNode = new RemoteLinkRootNode(path, '/', this);
    nodes['/'] = rootNode;
    if (rootNodeData != null) {
      rootNode.load(rootNodeData);
    }

    startNodeCleaner();
  }

  Map<String, Responder> responders;

  /// multiple-requester is allowed, like from different browser tabs
  /// in this case they need multiple responders on broker side.
  Responder getResponder(NodeProvider nodeProvider, String dsId, [String sessionId = '']) {
    if (responders == null) {
      responders = {};
    }
    if (responders.containsKey(sessionId)) {
      return responders[sessionId]..reqId = dsId;
    } else {
      var responder = nodeProvider.createResponder(dsId, sessionId);
      responder.reqId = dsId;
      //TODO set permission group from user node
      responder.updateGroups([]);
      responders[sessionId] = responder;
      return responder;
    }
  }

  LocalNode getNode(String fullPath) {
    String rPath = fullPath.replaceFirst(path, '');
    if (rPath == '') {
      rPath = '/';
    }
    return nodes[rPath];
  }

  LocalNode getOrCreateNode(String fullPath, [bool addToTree = true]) {
    if (addToTree == true) {
      throw 'not supported';
    }

    String rPath = fullPath.replaceFirst(path, '');
    if (rPath == '') {
      rPath = '/';
    }
    RemoteLinkNode node = nodes[rPath];
    if (node == null) {
      node = new RemoteLinkNode(fullPath, broker, rPath, this);
      nodes[rPath] = node;
    }
    return node;
  }

  /// get an existing node or create a dummy node for requester to listen on
  LocalNode operator [](String path) {
    return getNode(path);
  }

  RemoteNode getRemoteNode(String rPath) {
    String fullPath = path + rPath;
    if (rPath == '') {
      rPath = '/';
    }
    RemoteLinkNode node = nodes[rPath];
    if (node == null) {
      node = new RemoteLinkNode(fullPath, broker, rPath, this);
      nodes[rPath] = node;
    }
    return node;
  }

  Node getDefNode(String rPath, String defName) {
    if (DefaultDefNodes.nameMap.containsKey(defName)) {
      return DefaultDefNodes.nameMap[defName];
    }
    // reuse local broker node and doesn't reload it
    if (rPath.startsWith('/defs/') && broker.nodes.containsKey(rPath)) {
      LocalNode node = broker.nodes[rPath];
      if (node is LocalNodeImpl && node.loaded) {
        return node;
      }
    }
    return getRemoteNode(rPath);
  }

  RemoteNode updateRemoteChildNode(RemoteNode parent, String name, Map m) {
    String path;
    if (parent.remotePath == '/') {
      path = '/$name';
    } else {
      path = '${parent.remotePath}/$name';
    }
    if (parent is RemoteLinkNode) {
      RemoteLinkNode node = parent._linkManager.getRemoteNode(path);
      node.updateRemoteChildData(m, this);
      return node;
    }
    return null;
  }

  LocalNode operator ~()=> this['/'];

  IPermissionManager get permissions => broker.permissions;

  Responder createResponder(String dsId, String sessionId) {
    throw 'not implemented';
  }

  @override
  void clear() {
    nodes.clear();
  }

  @override
  void clearCachedNode(String path) {
    nodes.remove(path);
  }

  @override
  bool isNodeCached(String path) {
    return nodes.containsKey(path);
  }

  @override
  void startNodeCleaner() {
    if (_cleanerTimer != null) {
      return;
    }

    _cleanerTimer = Scheduler.every(Interval.FOUR_SECONDS, () {
      clearDanglingNodes();
    });
  }

  @override
  void stopNodeCleaner() {
    if (_cleanerTimer == null) {
      return;
    }

    _cleanerTimer.cancel();
    _cleanerTimer = null;
  }

  Timer _cleanerTimer;

  @override
  void clearDanglingNodes([bool handler(RemoteNode node)]) {
    List<String> toRemove = [];
    for (String key in nodes.keys) {
      RemoteNode node = nodes[key];
      if (node.referenceCount == 0) {
        toRemove.add(key);
      }
    }

    for (String key in toRemove) {
      logger.fine("Clearing dangling remote node at ${key}");
      clearCachedNode(key);
    }
  }
}

class RemoteLinkNode extends RemoteNode implements LocalNode {
  /// storage bucket for override attributes
  static IValueStorageBucket storageBucket;

  final BrokerNodeProvider provider;

  ListController createListController(Requester requester) {
    return new RemoteLinkListController(this, requester);
  }

  BroadcastStreamController<String> _listChangeController;

  BroadcastStreamController<String> get listChangeController {
    if (_listChangeController == null) {
      _listChangeController = new BroadcastStreamController<String>(
          onStartListListen, onAllListCancel);
    }
    return _listChangeController;
  }

  Stream<String> get listStream => listChangeController.stream;

  StreamSubscription _listReqListener;

  void onStartListListen() {
    if (_listReqListener == null) {
      _listReqListener =
      _linkManager.requester.list(remotePath).listen(_onListUpdate);
    }
  }

  void onAllListCancel() {
    if (_listReqListener != null) {
      _listReqListener.cancel();
      _listReqListener = null;
    }
    _listReady = false;
  }

  void _onListUpdate(RequesterListUpdate update) {
    for (var change in update.changes) {
      listChangeController.add(change);
    }
    _listReady = true;
  }

  Map<Function, int> callbacks = new Map<Function, int>();

  RespSubscribeListener subscribe(callback(ValueUpdate), [int qos = 0]) {
    callbacks[callback] = qos;
    var rslt = new RespSubscribeListener(this, callback);
    if (valueReady) {
      callback(_lastValueUpdate);
    }
    _linkManager.requester.subscribe(remotePath, updateValue, qos);
    return rslt;
  }

  void unsubscribe(callback(ValueUpdate)) {
    if (callbacks.containsKey(callback)) {
      callbacks.remove(callback);
    }
    if (callbacks.isEmpty) {
      _linkManager.requester.unsubscribe(remotePath, updateValue);
      _valueReady = false;
    }
  }

  ValueUpdate _lastValueUpdate;

  ValueUpdate get lastValueUpdate {
    return _lastValueUpdate;
  }

  /// Gets the current value of this node.
  dynamic get value {
    if (_lastValueUpdate != null) {
      return _lastValueUpdate.value;
    }
    return null;
  }

  void updateValue(Object update, {bool force: false}) {
    if (update is ValueUpdate) {
      _lastValueUpdate = update;
      callbacks.forEach((callback, qos) {
        callback(_lastValueUpdate);
      });
    } else if (_lastValueUpdate == null || _lastValueUpdate.value != update || force) {
      _lastValueUpdate = new ValueUpdate(update);
      callbacks.forEach((callback, qos) {
        callback(_lastValueUpdate);
      });
    }
    _valueReady = true;
  }
  void clearValue() {
    _valueReady = false;
    _lastValueUpdate = null;
  }

  final String path;

  /// root of the link
  RemoteLinkManager _linkManager;

  RemoteLinkNode(this.path, this.provider, String remotePath, this._linkManager)
  : super(remotePath) {
  }

  bool _listReady = false;

  /// whether broker is already listing, can send data directly for new list request
  bool get listReady => _listReady;

  String get disconnected => _linkManager.disconnected;

  bool _valueReady = false;

  /// whether broker is already subscribing, can send value directly for new subscribe request
  bool get valueReady => _valueReady;

  bool get exists => true;

  /// requester invoke function
  InvokeResponse invoke(
      Map params, Responder responder, InvokeResponse response, LocalNode parentNode, [int maxPermission = Permission.CONFIG]) {
    // TODO, when invoke closed without any data, also need to updateStream to close
    StreamSubscription sub = _linkManager.requester
    .invoke(remotePath, params)
    .listen((RequesterInvokeUpdate update) {
      if (update.error != null) {
        response.close(update.error);
      } else {
        response.updateStream(update.updates,
        streamStatus: update.streamStatus, columns: update.rawColumns, meta:update.meta);
      }
    }, onDone: () {
      response.close();
    });
    response.onReqParams = (Map m){
      // TODO
    };
    response.onClose = (InvokeResponse rsp) {
      sub.cancel();
    };
    return response;
  }

  Node getChild(String name) {
    return _linkManager.getOrCreateNode('$path/$name', false);
  }

  /// for invoke permission as responder
  int getInvokePermission() {
    return Permission.parse(getConfig(r'$invokable'), Permission.READ);
  }

  /// for invoke permission as responder
  int getSetPermission() {
    return Permission.parse(getConfig(r'$writable'), Permission.WRITE);
  }

  Response removeAttribute(
      String name, Responder responder, Response response) {
    // TODO check permission on RemoteLinkRootNode
    _linkManager.requester.remove(remotePath).then((update) {
      response.close();
    }).catchError((err) {
      if (err is DSError) {
        response.close(err);
      } else {
        // TODO need a broker setting to disable detail
        response.close(new DSError('internalError', detail: '$err'));
      }
    });
    return response;
  }

  Response removeConfig(String name, Responder responder, Response response) {
    // TODO check permission on RemoteLinkRootNode
    _linkManager.requester.remove(remotePath).then((update) {
      response.close();
    }).catchError((err) {
      if (err is DSError) {
        response.close(err);
      } else {
        // TODO need a broker setting to disable detail
        response.close(new DSError('internalError', detail: '$err'));
      }
    });
    return response;
  }

  Response setAttribute(
      String name, Object value, Responder responder, Response response) {
    if (value is Map && value['@'] != null) {
      overrideAttributeChanged(name, value);
      return response..close();
    }
    // TODO check permission on RemoteLinkRootNode
    _linkManager.requester.set('$remotePath/$name', value).then((update) {
      response.close();
    }).catchError((err) {
      if (err is DSError) {
        response.close(err);
      } else {
        // TODO need a broker setting to disable detail
        response.close(new DSError('internalError', detail: '$err'));
      }
    });
    return response;
  }

  Response setConfig(
      String name, Object value, Responder responder, Response response) {
    // TODO check permission on RemoteLinkRootNode
    _linkManager.requester.set('$remotePath/$name', value).then((update) {
      response.close();
    }).catchError((err) {
      if (err is DSError) {
        response.close(err);
      } else {
        // TODO need a broker setting to disable detail
        response.close(new DSError('internalError', detail: '$err'));
      }
    });
    return response;
  }

  Response setValue(Object value, Responder responder, Response response, [int maxPermission = Permission.CONFIG]) {
    // TODO check permission on RemoteLinkRootNode
    _linkManager.requester.set(remotePath, value, maxPermission).then((update) {
      response.close();
    }).catchError((err) {
      if (err is DSError) {
        response.close(err);
      } else {
        // TODO need a broker setting to disable detail
        response.close(new DSError('internalError', detail: '$err'));
      }
    });
    return response;
  }

  Map _lastChildData;

  void updateRemoteChildData(Map m, RemoteNodeCache cache) {
    _lastChildData = m;
    super.updateRemoteChildData(m, cache);
  }

  /// get simple map should return all configs returned by remoteNode
  Map getSimpleMap() {
    Map m = super.getSimpleMap();
    if (_lastChildData != null) {
      _lastChildData.forEach((String key, value) {
        if (key.startsWith(r'$')) {
          m[key] = this.configs[key];
        }
      });
    }
    return m;
  }

  bool get hasSubscriber {
    return callbacks.isNotEmpty;
  }

  operator [](String name) {
    return get(name);
  }

  operator []=(String name, Object value) {
    if (name.startsWith(r"$")) {
      configs[name] = value;
    } else if (name.startsWith(r"@")) {
      attributes[name] = value;
    } else if (value is Node) {
      addChild(name, value);
    }
  }


  IValueStorage _attributeStorage;
  Map _overrideAttributes;
  Map _downstreamAttributes;
  /// override attribute change from the broker
  void overrideAttributeChanged(String name, Map value) {
    if (_attributeStorage == null && storageBucket != null) {
      _attributeStorage = storageBucket.getValueStorage(path);
    }
    bool changed = false;
    Object method = value['@'];
    if  (method == 'overwrite' || method == 'merge' ) {
      changed = true;
      if (_overrideAttributes == null) {
        _overrideAttributes = {};
        _downstreamAttributes = {};
      }
      if (!_overrideAttributes.containsKey(name)) {
        _overrideAttributes[name] = value;
        if (attributes[name] != null) {
          _downstreamAttributes = attributes[name];
        }
      } else if (method == 'merge') {
        Map m = _overrideAttributes[name];
        if (m['@'] == 'merge' && m['type'] == value['type']) {
          if (m['val'] is List && value['val'] is List) {
            // merge 2 list
            List l = value['val'].toList();
            for (Object obj in m['val']) {
              if (!l.contains(obj)) {
                l.add(obj);
              }
            }
            m['val'] = l;
          } else if (m['val'] is Map && value['val'] is Map) {
            // merge 2 map
            (value['val'] as Map).forEach((k,v){
              m['val'][k] = v;
            });
          } else {
            _overrideAttributes[name] = value;
          }
        } else {
          _overrideAttributes[name] = value;
        }
      } else {
        // overwrite
        _overrideAttributes[name] = value;
      }
      if (_attributeStorage != null) {
        _attributeStorage.setValue(_overrideAttributes);
      }
      mergeOverrideAttribute(name);
    } else if (method == 'remove') {
      if (_overrideAttributes != null && _overrideAttributes.containsKey(name)) {
        Map m = _overrideAttributes[name];
        if (m['type'] == value['type'] ) {
          if (m['val'] is List && value['val'] is List) {
            for (Object obj in value['val']) {
              if (m['val'].contains(obj)) {
                m['val'].remove(obj);
                changed = true;
              }
            }
          } else if (m['val'] is Map && value['keys'] is List) {
            for (String key in value['keys']) {
              (value['val'] as Map).remove(key);
              changed = true;
            }
          }
        }
      }
    } else if (method == 'clear') {
      if (_overrideAttributes != null && _overrideAttributes.containsKey(name)) {
        _overrideAttributes.remove(name);
        if (_attributeStorage != null) {
          if (_overrideAttributes.isEmpty) {
            _attributeStorage.destroy();
          } else {
            _attributeStorage.setValue(_overrideAttributes);
          }
        }

        if (_downstreamAttributes.containsKey(name)) {
          attributes[name] = _downstreamAttributes[name];
        }
        changed = true;
      }
    }
    if (changed) {
      listChangeController.add(name);
    }
  }
  /// attribute change update from downstream
  bool downstreamAttributeChanged(String name, Object value) {
    if (_overrideAttributes != null && _overrideAttributes.containsKey(name)) {
      _downstreamAttributes[name] = value;
      if (_overrideAttributes[name]['@'] == 'overwrite') {
        return false;
      }
      mergeOverrideAttribute(name);
      return true;
    } else {
      if (value == null) {
        attributes.remove(name);
      } else {
        attributes[name] = value;
      }
      return true;
    }
  }
  /// merge the attribute from downstream
  void mergeOverrideAttribute(name) {
    if (_overrideAttributes.containsKey(name)) {
      if (_downstreamAttributes.containsKey(name)) {
        Map m = _overrideAttributes[name];
        if (m['@'] == 'merge') {
          Map rsltMap = {'@':'merge' ,'type':m['type']};
          Object valo = m['val'];
          Object vald = _downstreamAttributes[name];
          if (vald is Map && vald['@'] != null) {
            vald = (vald as Map)['val'];
          }
          if (valo is List && vald is List) {
            List rslt = valo.toList();
            if (m['type'] == 'paths') {
              rslt.addAll(vald.map((str)=>'${_linkManager.path}$str'));
            } else {
              rslt.addAll(vald);
            }
            rsltMap['val'] = rslt;
          } else if (valo is Map && vald is Map) {
            Map rslt = {};
            vald.forEach((k,v) {
              rslt[k] = v;
            });
            valo.forEach((k,v) {
              rslt[k] = v;
            });
            rsltMap['val'] = rslt;
          }
          attributes[name] = rsltMap;
        } else/* if (m['@'] == 'overwrite') */{
          attributes[name] = _overrideAttributes[name];
        }
      } else {
        attributes[name] = _overrideAttributes[name];
      }
    } else {
      if (_downstreamAttributes.containsKey(name)) {
        attributes[name] = _downstreamAttributes[name];
      } else {
        return;
      }
    }
  }
  /// initialization of override attributes
  void updateOverrideAttributes(Map m) {
    _overrideAttributes = m;
    _downstreamAttributes = {};
  }

  /// reset node cache when remote list api require a reset of the node data
  /// this is done by a new $is in list update
  void resetNodeCache(){
    configs.clear();
    attributes.clear();
    children.clear();
    if (_overrideAttributes != null) {
      _overrideAttributes.forEach((k,v) {
        attributes[k] = v;
      });
    }
  }
}

class RemoteLinkListController extends ListController {
  RemoteLinkListController(RemoteNode node, Requester requester) : super(node, requester);

  void onUpdate(String streamStatus, List updates, List columns, Map meta, DSError error) {
    bool reseted = false;
    // TODO: implement error handling
    if (updates != null) {
      for (Object update in updates) {
        String name;
        Object value;
        bool removed = false;
        if (update is Map) {
          if (update['name'] is String) {
            name = update['name'];
          } else {
            continue;
            // invalid response
          }
          if (update['change'] == 'remove') {
            removed = true;
          } else {
            value = update['value'];
          }
        } else if (update is List) {
          if (update.length > 0 && update[0] is String) {
            name = update[0];
            if (update.length > 1) {
              value = update[1];
            }
          } else {
            continue;
            // invalid response
          }
        } else {
          continue;
          // invalid response
        }
        if (name.startsWith(r'$')) {
          if (!reseted && (name == r'$is' || name == r'$base' || (name == r'$disconnectedTs' && value is String))) {
            reseted = true;
            node.resetNodeCache();
          }

          if (name == r'$base' && value is String) {
            value = (node as RemoteLinkNode)._linkManager.path + value;
          }
          if (name == r'$is' && !node.configs.containsKey(r'$base')) {
            node.configs[r'$base'] = (node as RemoteLinkNode)._linkManager.path;
            changes.add(r'$base');
          }
          changes.add(name);
          if (removed) {
            node.configs.remove(name);
          } else {
            node.configs[name] = value;
          }
        } else if (name.startsWith('@')) {
          if (removed) {
            if ((node as RemoteLinkNode).downstreamAttributeChanged(name, null) ) {
              changes.add(name);
            }
          } else {
            if ((node as RemoteLinkNode).downstreamAttributeChanged(name, value) ) {
              changes.add(name);
            }
          }
        } else {
          changes.add(name);
          if (removed) {
            node.children.remove(name);
          } else if (value is Map) {
            node.children[name] =
            requester.nodeCache.updateRemoteChildNode(node, name, value);
          }
        }
      }
      if (request.streamStatus != StreamStatus.initialize) {
        node.listed = true;
      }
      onProfileUpdated();
    }
  }
}
