part of dsbroker.broker;

class UserNode extends BrokerNode {
  final String username;
  UserNode(String path, BrokerNodeProvider provider, this.username) :
      super(path, provider) {
    configs[r'$is'] = 'broker/userNode';
    profile = provider.getOrCreateNode('/defs/profile/broker/userNode', false);
  }

  BrokerNode parent;
  bool _loaded = false;
  /// Load this node from the provided map as [m].
  void load(Map m) {
    if (_loaded) {
      configs.clear();
      attributes.clear();
      children.clear();
    }
    String childPathPre;
    if (path == '/') {
      childPathPre = '/';
    } else {
      childPathPre = '$path/';
    }
    m.forEach((String key, value) {
      if (key.startsWith(r'$')) {
        configs[key] = value;
      } else if (key.startsWith('@')) {
        attributes[key] = value;
      } else if (value is Map) {
        String childPath = '$childPathPre$key';
        LocalNode node = provider.getOrCreateNode(childPath, false);
        if (node is UserNode) {
          node.parent = this;
        }

        children[key] = node;
        if (node is UserNode) {
          node.load(value);
        } else if (node is RemoteLinkRootNode) {
          node.load(value);
          node._linkManager.inTree = true;
          if (node.configs[r'$$dsId'] is String) {
            String userDsId = '$username:${node.configs[r'$$dsId']}';
            provider._id2connPath[userDsId] = childPath;
            provider._connPath2id[childPath] = userDsId;
          }
        }
      }
    });
    _loaded = true;
  }
}

class UserRootNode extends UserNode {
  UserRootNode(String path, String username, BrokerNodeProvider provider)
      : super(path, provider, username) {
    configs[r'$is'] = 'broker/userRoot';
    profile = provider.getOrCreateNode('/defs/profile/broker/userRoot', false);
  }
}

InvokeResponse addUserChildNode(Map params, Responder responder,
    InvokeResponse response, LocalNode parentNode) {
  Object name = params['Name'];
  if (parentNode is UserNode &&
      name is String &&
      name != '' &&
      !name.contains(Path.invalidNameChar) &&
      !name.startsWith(r'$') &&
      !name.startsWith(r'!') &&
      !name.startsWith(r'#')) {
    if (parentNode.children.containsKey(name)) {
      return response
        ..close(new DSError('invalidParameter', msg: 'node already exist'));
    }

    UserNode node = responder.nodeProvider.getOrCreateNode(
      '${parentNode.path}/$name',
      false
    );
    node.parent = parentNode;
    parentNode.children[name] = node;
    parentNode.updateList(name);

    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveUsrNodes,
      1000
    );
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse addUserLink(
  Map params,
  Responder responder,
  InvokeResponse response,
  LocalNode parentNode) {
  Object name = params['Name'];
  Object dsId = params['Id'];
  if (parentNode is UserNode &&
      name is String &&
      name != '' &&
      !name.contains(Path.invalidNameChar) &&
      !name.startsWith(r'$') &&
      !name.startsWith('!')) {
    if (!(name as String).startsWith('#')) {
      name = '#$name';
    }
    if (parentNode.children.containsKey(name)) {
      return response
        ..close(new DSError('invalidParameter', msg: 'node already exist'));
    }
    String userDsId = '${parentNode.username}:$dsId';
    String existingPath = parentNode.provider._id2connPath[userDsId];
    if (existingPath != null && existingPath.startsWith('/users/')) {
      return response
             ..close(new DSError('invalidParameter', msg: 'id already in use'));
    }
    String path = '${parentNode.path}/$name';
    parentNode.provider._id2connPath[userDsId] = path;
    parentNode.provider._connPath2id[path] = userDsId;

    ServerLink link = parentNode.provider.getLinkAndConnectNode(userDsId);
    if (link != null) {
      link.close();
      parentNode.provider.removeLink(link, link.dsId);
    }
    LocalNode node = responder.nodeProvider.getOrCreateNode(path, false);
    node.configs[r'$$dsId'] = dsId;
    parentNode.children[name] = node;
    parentNode.updateList(name);
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveUsrNodes,
      1000
    );
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse removeUserNode(
  Map params,
  Responder responder,
  InvokeResponse response,
  LocalNode parentNode) {
  Object recursive = params['Recursive'];
  if (parentNode is UserNode &&
    parentNode is! UserRootNode &&
    parentNode.parent != null // make sure parent node itself is in tree
  ) {
    if (recursive == true) {
      removeUserNodeRecursive(parentNode,
        parentNode.path.substring(parentNode.path.lastIndexOf('/') + 1));
    } else {
      if (parentNode.children.isEmpty) {
        UserNode parent = parentNode.parent;
        String name = parentNode.path.substring(
          parentNode.path.lastIndexOf('/') + 1);
        parentNode.parent = null;
        parentNode.attributes.clear();
        parent.children.remove(name);
        parent.updateList(name);
        parentNode.clearValue();
      } else {
        return response..close(DSError.INVALID_PARAMETER);
      }
    }
    DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveUsrNodes, 1000);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

void removeUserNodeRecursive(UserNode node, String name) {
  for (String name in node.children.keys.toList()) {
    removeUserNodeRecursive(node.children[name], name);
  }
  node.attributes.clear();
  UserNode parent = node.parent;
  node.parent = null;
  parent.children.remove(name);
  parent.updateList(name);
  node.clearValue();
}

final Map<String, dynamic> _userNodeFunctions = <String, dynamic>{
  "broker": {
    "userNode": {
      "addChild": addUserChildNode,
//      "addLink": addUserLink,
      "removeNode": removeUserNode
    },
    "userRoot": {
      "addChild": addUserChildNode
//      "addLink": addUserLink
    }
  }
};
