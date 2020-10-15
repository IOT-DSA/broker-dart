part of dsbroker.broker;

class TokenContext {
  final BrokerNodeProvider broker;

  TokenContext(this.broker);

  Map<String, TokenNode> tokens = new Map<String, TokenNode>();
  Map<String, TokenNode> _trustedTokens = {};
  Map<String, TokenNode> get trustedTokens => _trustedTokens;

  // a token map used by both global tokens, and user tokens
  TokenNode createTrustedToken(
    String name, BrokerNodeProvider provider) {
    if (_trustedTokens.containsKey(name)) {
      return _trustedTokens[name];
    }

    String token = makeToken();
    String tokenId = token.substring(0, 16);
    TokenNode node = new TokenNode(null, provider, null, tokenId);
    node.configs[r'$$token'] = token;
    node.init();
    tokens[tokenId] = node;
    _trustedTokens[name] = node;
    return node;
  }

  String makeToken() {
    List<int> tokenCodes = new List<int>(48);
    int i = 0;
    while (i < 48) {
      int n = DSRandom.instance.nextUint8();
      if ((n >= 0x30 && n <= 0x39) ||
        (n >= 0x41 && n <= 0x5A) ||
        (n >= 0x61 && n <= 0x7A)) {
        tokenCodes[i] = n;
        i++;
      }
    }
    String rslt = new String.fromCharCodes(tokenCodes);
    String short = rslt.substring(0, 16);
    if (tokens.containsKey(short)) {
      return makeToken();
    }
    return rslt;
  }

  TokenNode findTokenNode(String token, String dsId) {
    if (token.length < 16) {
      return null;
    }

    String tokenId = token.substring(0, 16);
    String tokenHash = token.substring(16);
    TokenNode tokenNode = tokens[tokenId];
    if (tokenNode == null || tokenNode.token == null || tokenNode.count == 0) {
      return null;
    }

    if (tokenNode.ts0 != null && tokenNode.ts1 != null) {
      var now = new DateTime.now();
      if (now.isBefore(tokenNode.ts0) || now.isAfter(tokenNode.ts1)) {
        return null;
      }
    }

    String hashStr = CryptoProvider
      .sha256(const Utf8Encoder().convert('$dsId${tokenNode.token}'));
    if (hashStr == tokenHash) {
      return tokenNode;
    }
    return null;
  }
}

class TokenGroupNode extends BrokerStaticNode {
  String groupId;

  TokenGroupNode(String path, BrokerNodeProvider provider, this.groupId)
      : super(path, provider) {
    configs[r'$is'] = 'broker/tokenGroup';
    profile =
        provider.getOrCreateNode('/defs/profile/broker/tokenGroup', false);
  }

  void init() {}

  bool _loaded = false;

  void load(Map m) {
    if (_loaded) {
      configs.clear();
      attributes.clear();
      children.clear();
    }

    m.forEach((String key, value) {
      if (key.startsWith(r'$')) {
        configs[key] = value;
      } else if (key.startsWith('@')) {
        attributes[key] = value;
      } else if (value is Map) {
        TokenNode node = new TokenNode('$path/$key', provider, this, key);
        provider.tokenContext.tokens[key] = node;
        node.load(value);
        children[key] = node;
      }
    });
    _loaded = true;
  }
}

class TokenNode extends BrokerNode {
  DateTime ts0;
  DateTime ts1;
  int count = -1;

  /// destroy token with a timer;
  Timer timer;

  // when true, kill all dslink when token is removed
  bool managed = false;

  List links;

  TokenGroupNode parent;
  String id;
  String token;
  String group;

  TokenNode(String path, BrokerNodeProvider provider, this.parent, this.id)
      : super(path, provider) {
    configs[r'$is'] = 'broker/token';
    profile = provider.getOrCreateNode('/defs/profile/broker/token', false);
    if (path != null) {
      // trustedTokenNode is not stored in the tree
      provider.setNode(path, this);
    }
  }

  void load(Map m) {
    super.load(m);
    init();
  }

  /// initialize timeRange and count
  void init() {
    if (configs[r'$$timeRange'] is String) {
      _loadTimes();
    }

    if (configs[r'$$count'] is num) {
      count = (configs[r'$$count'] as num).toInt();
    }

    if (configs[r'$$managed'] == true) {
      managed = true;
      if (configs[r'$$links'] is List) {
        links = configs[r'$$links'];
      }
    }

    if (configs[r'$$token'] is String) {
      token = configs[r'$$token'];
    }

    if (configs[r'$$group'] is String) {
      group = configs[r'$$group'];
    }

    // TODO: implement target position
    // TODO: when target position is gone, token should be removed
  }

  void _loadTimes() {
    String s = configs[r'$$timeRange'];
    List dates = s.split('/');

    if (dates.length != 2) {
      logger.info('Failed to prase Token TimeRange: $s');
      return;
    }

    try {
      ts0 = DateTime.parse(dates[0]);
      ts1 = DateTime.parse(dates[1]);
    } catch (err) {
      logger.info('Failed to parse Token TimeRange: $s', err);
      return;
    }

    // Set timer to delete token if it is not yet expired.
    DateTime now = new DateTime.now();
    if (now.isBefore(ts1)) {
      timer = new Timer(ts1.difference(now), delete);
    } else {
      DsTimer.callLater(delete);
    }
  }

  /// get the node where children should be connected
  BrokerNode getTargetNode() {
    // TODO: allow user to define the target node for his own token
    return provider.connsNode;
  }

  /// return true if link is managed by token
  bool useToken(String path) {
    if (count > 0 || managed) {
      DsTimer.timerOnceBefore(provider.saveTokensNodes, 1000);
    }

    if (count > 0) {
      count--;
      configs[r'$$count'] = count;
      updateList(r'$$count');
    }

    if (managed) {
      if (links == null) {
        links = [];
        configs[r'$$links'] = links;
      }
      if (!links.contains(path)) {
        links.add(path);
      }
      updateList(r'$$links');
      return true;
    }
    return false;
  }

  void delete() {
    deleteLinks();
    parent.children.remove(id);
    provider.tokenContext.tokens.remove(id);
    parent.updateList(id);
    provider.clearNode(this);
    DsTimer.timerOnceBefore(provider.saveTokensNodes, 1000);
  }

  void deleteLinks() {
    if (links != null) {
      for (Object path in links) {
        if (path is String) {
          Object node = provider.getNode(path);
          if (node is RemoteLinkRootNode) {
            Object token = node.configs[r'$$token'];
            if (token == id) {
              provider.removeLinkByPath(node.path);
            }
          }
        }
      }
    }
    links = null;
  }

  void updateLinks() {
    if (links == null) return;

    var updated = false;
    for (Object path in links) {
      if (path is! String) continue;
      Object nd = provider.getNode(path as String);
      if (nd is! RemoteLinkRootNode) continue;
      (nd as RemoteLinkRootNode).configs[r'$$group'] = group;
      (nd as RemoteLinkRootNode).updateList(r'$$group');
      updated = true;
    }

    if (updated) {
      DsTimer.timerOnceBefore(provider.saveConns, 300);
    }
  }
}

InvokeResponse _deleteTokenNode(Map params, Responder responder,
    InvokeResponse response, LocalNode parentNode) {
  if (parentNode is TokenNode) {
    parentNode.delete();
    parentNode.provider.logConfigMessage('delete token: ${parentNode.token}', responder);
    return response..close();
  }
  return response..close(DSError.INVALID_PARAMETER);
}

InvokeResponse _updateTokenNode(Map params, Responder responder,
    InvokeResponse response, LocalNode parentNode) {
  if (parentNode is! TokenNode) {
    return response..close(DSError.INVALID_PARAMETER);
  }

  var group = params['Group'] as String;
  if (group == null || group.isEmpty) {
    return response..close(DSError.INVALID_PARAMETER);
  }

  var node = parentNode as TokenNode;
  node..configs[r'$$group'] = group
      ..group = group
      ..updateList(r'$$group')
      ..updateLinks();

  DsTimer.timerOnceBefore(
      (responder.nodeProvider as BrokerNodeProvider).saveTokensNodes, 1000);
  node.provider.logConfigMessage(
      'updated token: ${node.token} to group: $group',
      responder);
  return response..close();
}

InvokeResponse _addTokenNode(Map params, Responder responder,
    InvokeResponse response, LocalNode parentNode) {
  if (params == null) {
    params = {};
  }

  if (parentNode is TokenGroupNode) {
    String token = parentNode.provider.tokenContext.makeToken();
    String tokenId = token.substring(0, 16);
    TokenNode node = new TokenNode('${parentNode.path}/$tokenId',
        parentNode.provider, parentNode, tokenId);
    node.configs[r'$$timeRange'] = params['TimeRange'];
    node.configs[r'$$count'] = params['Count'];
    node.configs[r'$$managed'] = params['Managed'];
    // TODO check group
    node.configs[r'$$group'] = params['Group'];
    node.configs[r'$$token'] = token;
    node.init();
    parentNode.provider.tokenContext.tokens[tokenId] = node;
    parentNode.children[tokenId] = node;
    parentNode.updateList(tokenId);

    response.updateStream([
      [token]
    ], streamStatus: StreamStatus.closed);
    DsTimer.timerOnceBefore(
        (responder.nodeProvider as BrokerNodeProvider).saveTokensNodes, 1000);
    node.provider.logConfigMessage('add new token: $token', responder);
    return response;
  }
  return response..close(DSError.INVALID_PARAMETER);
}

final Map<String, dynamic> _tokenNodeFunctions = <String, dynamic>{
  "broker": {
    "token": {
      "delete": _deleteTokenNode,
      "update": _updateTokenNode
    },
    "tokenGroup": {
      "add": _addTokenNode
    }
  }
};
