part of dsbroker.broker;

class BrokerDataNode extends BrokerNode {
  IValueStorage storage;
  BrokerNode parent;

  BrokerDataNode(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    if (provider.dataNode != null && provider.dataNode.storageBucket != null) {
      storage = provider.dataNode.storageBucket.getValueStorage(path);
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
      DsTimer.timerOnceBefore(
        (responder.nodeProvider as BrokerNodeProvider).saveDataNodes, 1000);
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

  Response setAttribute(String name, Object value, Responder responder,
    Response response) {
    if (!attributes.containsKey(name) || attributes[name] != value) {
      attributes[name] = value;
      updateList(name);
      persist();
    }
    return response..close();
  }

  bool persist() {
    DsTimer.timerOnceBefore(
      provider.saveDataNodes,
      1000
    );
    return true;
  }
}

class BrokerDataRoot extends BrokerDataNode {
  IValueStorageBucket storageBucket;
  BrokerNode parent;

  BrokerDataRoot(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    configs[r'$is'] = 'broker/dataRoot';
    configs.remove(r'$writable');
    profile = provider.getOrCreateNode('/defs/profile/broker/dataRoot', false);
    // avoid parent checking
    parent = this;
  }

  void init() {
    storage = storageBucket.getValueStorage(path);
  }
}

InvokeResponse _addDataNode(Map params, Responder responder,
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
        ..close(new DSError('invalidParameter', msg: 'node already exists'));
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
      node.updateValue(null);
    }
    parentNode.children[name] = node;
    node.parent = parentNode;
    parentNode.updateList(name);
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveDataNodes,
      1000
    );
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse _deleteDataNode(Map params, Responder responder,
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
        parentNode.attributes.clear();
        if (parentNode.storage != null) {
          parentNode.storage.destroy();
        }
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

InvokeResponse _renameDataNode(Map params, Responder responder,
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

InvokeResponse _duplicateDataNode(Map params, Responder responder,
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

BrokerDataNode cloneNodes(BrokerDataNode oldNode, BrokerDataNode newParent,
  String name) {
  BrokerDataNode node = newParent.provider.getOrCreateNode(
    '${newParent.path}/$name', false);

  newParent.children[name] = node;
  node.parent = newParent;

  oldNode.children.forEach((k, n) {
    cloneNodes(n, node, k);
  });

  oldNode.configs.forEach((k, v) {
    node.configs[k] = v;
    node.updateList(k);
  });
  oldNode.attributes.forEach((k, v) {
    node.attributes[k] = v;
    node.updateList(k);
  });

  Object oldValue = oldNode.value;
  if (oldValue != null) {
    node.updateValue(oldValue);
    if (node is BrokerDataNode && node.storage != null) {
      node.storage.setValue(oldValue);
    }
  }
  newParent.updateList(name);

  return node;
}

void removeDataNodeRecursive(BrokerDataNode node, String name) {
  for (String name in node.children.keys.toList()) {
    removeDataNodeRecursive(node.children[name], name);
  }
  node.attributes.clear();
  if (node.storage != null) {
    node.storage.destroy();
  }
  BrokerDataNode parent = node.parent;
  node.parent = null;
  parent.children.remove(name);
  parent.updateList(name);
  node.clearValue();
}

InvokeResponse _publishDataNode(Map params, Responder responder,
  InvokeResponse response, LocalNode parentNode) {
  // return true when params are valid
  bool publishReqParams(InvokeResponse invokeResponse, Map m) {
    Object path = m['Path'];
    Object value = m['Value'];
    Object ts = m['Timestamp'];
    Object closeStream = m['CloseStream'];
    if (closeStream == true) {
      response..close();
    }
    if (path is String && path.startsWith('/data/')) {
      Path p = new Path(path);
      if (!p.isNode || !p.valid) {
        return false;
      }
      BrokerDataNode node = (parentNode.provider as BrokerNodeProvider)
        ._getOrCreateDataNode(p.path);
      if (ts is String && ts.length > 22) {
        int len = (ts as String).length;
        if (len > 29 && len < 35) {
          // fix ts with macro seconds
          ts = '${(ts as String).substring(0, 23)}${(ts as String).substring(len - 6)}';
        }

        try {
          DateTime.parse(ts);
          node.updateValue(new ValueUpdate(value, ts: ts));
        } catch (e) {
          return false;
        }
      }
      node.updateValue(value);
      return true;
    }
    return false;
  }

  if (parentNode is BrokerDataRoot && publishReqParams(response, params)) {
    response.onReqParams = publishReqParams;
    // leave the invoke open
    return response;
  }
  return response..close(DSError.INVALID_PARAMETER);
}

final Map<String, dynamic> _dataNodeFunctions = <String, dynamic>{
  "broker": {
    "dataNode": {
      "addNode": _addDataNode,
      "addValue": _addDataNode,
      "deleteNode": _deleteDataNode,
      "renameNode": _renameDataNode,
      "duplicateNode": _duplicateDataNode
    },
    "dataRoot": {
      "addNode": _addDataNode,
      "addValue": _addDataNode,
      "publish": _publishDataNode
    },
  }
};
