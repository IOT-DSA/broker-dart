part of dsbroker.broker;

class BrokerDataNode extends BrokerNode {
  static IValueStorageBucket storageBucket;

  IValueStorage storage;
  BrokerNode parent;

  BrokerDataNode(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    if (storageBucket != null) {
      storage = storageBucket.getValueStorage(path);
    }
    configs[r'$is'] = 'broker/dataNode';
    profile = provider.getOrCreateNode('/defs/profile/broker/dataNode', false);
    configs[r'$writable'] = 'write';
  }

  Response setValue(Object value, Responder responder, Response response,
    [int maxPermission = Permission.CONFIG]) {
    if (parent == null) {
      // add this node to tree and create all parent levels
      provider.getOrCreateNode(path, true);
    }
    if (storage != null &&
      (lastValueUpdate == null || lastValueUpdate.value != value)) {
      storage.setValue(value);
    }
    return super.setValue(value, responder, response, maxPermission);
  }

  @override
  void load(Map m) {
    super.load(m);
    children.forEach((String key, Node node) {
      if (node is BrokerDataNode) {
        node.parent = this;
      }
    });
  }
}

class BrokerDataRoot extends BrokerDataNode {
  BrokerNode parent;

  BrokerDataRoot(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    configs[r'$is'] = 'broker/dataRoot';
    profile = provider.getOrCreateNode('/defs/profile/broker/dataRoot', false);
    // avoid parent checking
    parent = this;
  }
}

InvokeResponse addDataNode(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  Object name = params['Name'];
  Object type = params['Type'];
  Object editor = params['Editor'];
  if (parentNode is BrokerDataNode &&
    parentNode.parent != null && // make sure parent node itself is in tree
    name is String &&
    name != '' &&
    !name.contains(Path.invalidNameChar) &&
    !name.startsWith(r'$') &&
    !name.startsWith(r'!')) {
    if (parentNode.children.containsKey(name)) {
      return response
        ..close(new DSError('invalidParameter', msg: 'node already exist'));
    }
    BrokerDataNode node = responder.nodeProvider.getOrCreateNode(
      '${parentNode.path}/$name', false);
    if (type is String &&
      const [
        'string',
        'number',
        'bool',
        'array',
        'map',
        'binary',
        'dynamic'
      ].contains(type)) {
      node.configs[r'$type'] = type;
      if (editor is String) {
        node.configs[r'$editor'] = editor;
      }
    }
    parentNode.children[name] = node;
    node.parent = parentNode;
    parentNode.updateList(name);
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveDataNodes, 1000);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse deleteDataNode(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  Object recursive = params['Recursive'];
  if (parentNode is BrokerDataNode &&
    parentNode is! BrokerDataRoot &&
    parentNode.parent != null // make sure parent node itself is in tree
  ) {
    if (recursive == true) {
      removeDataNodeRecursive(parentNode,
        parentNode.path.substring(parentNode.path.lastIndexOf('/') + 1));
    } else {
      if (parentNode.children.isEmpty) {
        BrokerDataNode parent = parentNode.parent;
        String name = parentNode.path.substring(
          parentNode.path.lastIndexOf('/') + 1);
        parentNode.parent = null;
        parent.children.remove(name);
        parent.updateList(name);
        parentNode.clearValue();
      } else {
        return response..close(DSError.INVALID_PARAMETER);
      }
    }
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveDataNodes, 1000);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse renameDataNode(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  Object name = params['Name'];
  if (parentNode is BrokerDataNode &&
    parentNode is! BrokerDataRoot &&
    parentNode.parent != null && // make sure parent node itself is in tree
    name is String && name != '' && !parentNode.children.containsKey(name)
  ) {
    cloneNodes(parentNode, parentNode.parent, name);
    removeDataNodeRecursive(parentNode,
          parentNode.path.substring(parentNode.path.lastIndexOf('/') + 1));
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveDataNodes, 1000);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse duplicateDataNode(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  Object name = params['Name'];
  if (parentNode is BrokerDataNode &&
    parentNode is! BrokerDataRoot &&
    parentNode.parent != null && // make sure parent node itself is in tree
    name is String && name != '' && !parentNode.children.containsKey(name)
  ) {
    cloneNodes(parentNode, parentNode.parent, name);
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveDataNodes, 1000);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

BrokerDataNode cloneNodes(BrokerDataNode oldNode, BrokerDataNode newParent, String name) {
  BrokerDataNode node = newParent.provider.getOrCreateNode(
    '${newParent.path}/$name', false);

  newParent.children[name] = node;
  node.parent = newParent;

  oldNode.children.forEach((k,n){
    cloneNodes(n, node, k);
  });

  oldNode.configs.forEach((k,v){
    node.configs[k] = v;
    node.updateList(k);
  });
  oldNode.attributes.forEach((k,v){
    node.attributes[k] = v;
    node.updateList(k);
  });

  newParent.updateList(name);

  return node;
}

void removeDataNodeRecursive(BrokerDataNode node, String name) {
  for (String name in node.children.keys.toList()) {
    removeDataNodeRecursive(node.children[name], name);
  }
  BrokerDataNode parent = node.parent;
  node.parent = null;
  parent.children.remove(name);
  parent.updateList(name);
  node.clearValue();
}

InvokeResponse publish(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  // return true when params are valid
  bool publishReqParams(Map m) {
    Object path = m['Path'];
    Object value = m['Value'];
    Object ts = m['Timestamp'];
    if (path is String && path.startsWith('/data/')) {
      Path p = new Path(path);
      if (!p.isNode || !p.valid) {
        return false;
      }
      BrokerDataNode node = (parentNode.provider as BrokerNodeProvider)._getOrCreateDataNode(path);
      if (ts is String && ts.length > 22) {
        if ((ts as String).length == 32) {
          // fix ts with macro seconds
          ts = '${(ts as String).substring(0,23)}${(ts as String).substring(26)}';
        }
        try {
          DateTime.parse(ts);
          node.updateValue(new ValueUpdate(value, ts:ts));
        }catch(e){
          return false;
        }
      }
      node.updateValue(value);
      return true;
    }
    return false;
  }

  if (parentNode is BrokerDataRoot && publishReqParams(params)) {
    response.onReqParams = publishReqParams;
    // leave the invoke open
    return response;
  }
  return response..close(DSError.INVALID_PARAMETER);
}



Map dataNodeFunctions = {
  "broker": {
    "dataNode": {
      "addNode": addDataNode,
      "addValue": addDataNode,
      "deleteNode": deleteDataNode,
      "renameNode": renameDataNode,
      "duplicateNode": duplicateDataNode
    },
    "dataRoot": {"addNode": addDataNode, "addValue": addDataNode, "publish":publish},
  }
};
