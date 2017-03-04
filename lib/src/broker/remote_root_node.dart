part of dsbroker.broker;

const List<String> _LINK_ROOT_ALLOWED = const [
  r"$uid"
];

// TODO, implement special configs and attribute merging
class RemoteLinkRootNode extends RemoteLinkNode with BrokerNodePermission implements BrokerNode {
  RemoteLinkRootNode(
      String path, String remotePath, RemoteLinkManager linkManager)
      : super(path, linkManager.broker, remotePath, linkManager);

  bool get loaded => true;

  bool get isBroker {
    return configs[r'$is'] == 'dsa/broker';
  }
  // TODO does this need parentNode?
  LocalNode parentNode;

  ListController createListController(Requester requester) {
    return new RemoteLinkRootListController(this, requester);
  }

  Response setConfig(
      String name, Object value, Responder responder, Response response) {
    var config = Configs.getConfig(name, profile);
    return response..close(config.setConfig(value, this, responder));
  }

  Response removeConfig(String name, Responder responder, Response response) {
    var config = Configs.getConfig(name, profile);
    return response..close(config.removeConfig(this, responder));
  }

  void load(Map m) {
    m.forEach((String name, Object value) {
      if (name.startsWith(r'$')) {
        configs[name] = value;
      } else if (name.startsWith('@')) {
        attributes[name] = value;
      } else if (value is Map) {
        pchildren[name] = new VirtualNodePermission()..load(value);
      }
    });

    if (m['?permissions'] is List) {
      loadPermission(m['?permissions']);
    }
  }

  Map serialize(bool withChildren) {
    Map rslt = {};
    configs.forEach((String name, Object val) {
      rslt[name] = val;
    });
    attributes.forEach((String name, Object val) {
      rslt[name] = val;
    });
    pchildren.forEach((String name, VirtualNodePermission val) {
      rslt[name] = val.serialize();
    });
    List permissionData = this.serializePermission();
    if (permissionData != null) {
      rslt['?permissions'] = permissionData;
    }
    return rslt;
  }

  void updateList(String name, [int permission = Permission.READ]) {
    listChangeController.add(name);
  }

  List getDisconnectedListResponse() {
    List rslt = [
      [r'$disconnectedTs', disconnected]
    ];
    if (configs.containsKey(r'$$group')) {
      rslt.add([r'$$group', configs[r'$$group']]);
    }
    return rslt;
  }

  void resetNodeCache() {
    children.clear();
    configs.remove(r'$disconnectedTs');
  }

  /// children list only for permissions
  Map<String, VirtualNodePermission> pchildren = new Map<String, VirtualNodePermission>();

  BrokerNodePermission getPermissionChild(String str) {
    return pchildren[str];
  }

  BrokerNodePermission getPermissionChildWithPath(String path, bool create) {
    if (path == '/') {
      return this;
    }
    List paths = path.split('/');
    return getPermissionChildWithPaths(this, paths, 1, create);
  }
  static BrokerNodePermission getPermissionChildWithPaths(BrokerNodePermission p, List paths, int pos, bool create) {
    if (pos >= paths.length) {
      return p;
    }
    String name = paths[pos];
    BrokerNodePermission pnext;
    if (name == '') {
      pnext = p;
    } else {
      pnext = p.getPermissionChild(name);
      if (pnext == null) {
        if (create) {
          pnext = new VirtualNodePermission();
          if (p is RemoteLinkRootNode) {
            p.pchildren[name] = pnext;
          } else if (p is VirtualNodePermission) {
            p.children[name] = pnext;
          } else {
            return null;
          }
        } else {
          return null;
        }
      }
    }
    return getPermissionChildWithPaths(pnext, paths, pos+1, create);
  }

  @override
  Map getSimpleMap() {
    Map m = super.getSimpleMap();
    if (configs.containsKey(r'$linkData')) {
      m[r'$linkData'] = configs[r'$linkData'];
    }
    return m;
  }

  bool persist() {
    DsTimer.timerOnceAfter(provider.saveConns, 3000);
    return true;
  }

  @override
  IValueStorage _attributeStore = null;
}

class RemoteLinkRootListController extends ListController {
  RemoteLinkRootListController(RemoteNode node, Requester requester)
      : super(node, requester);

  void onUpdate(String streamStatus, List updates, List columns, Map meta, DSError error) {
    bool reseted = false;
    // TODO implement error handling
    if (updates != null) {
      for (Object update in updates) {
        String name;
        Object value;
        bool removed = false;
        if (update is Map) {
          if (update['name'] is String) {
            name = update['name'];
          } else {
            continue; // invalid response
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
            continue; // invalid response
          }
        } else {
          continue; // invalid response
        }

        if (_LINK_ROOT_ALLOWED.contains(name) && (
          name.startsWith(r"$") || name.startsWith("@")
        )) {
          Map map = name.startsWith(r"$") ? node.configs : node.attributes;
          if (removed) {
            map.remove(name);
          } else {
            map[name] = value;
          }
          changes.add(name);
        }

        if (name.startsWith(r'$')) {
          if (!reseted &&
            (name == r'$is' ||
              name == r'$base' ||
              (name == r'$disconnectedTs' && value is String))) {
            if (name == r'$is') {
              if (value == 'dsa/broker') {
                node.configs[r'$is'] = 'dsa/broker';
              } else {
                node.configs[r'$is'] = 'dsa/link';
              }
            }
            reseted = true;
            node.resetNodeCache();
            changes.add(name);
          } else if (name.startsWith(r'$link_')) {
            node.configs[name] = value;
            changes.add(name);
          }
          // ignore other changes
        } else if (name.startsWith('@')) {
          node.attributes[name] = value;
          changes.add(name);

          if (node.attributes["@icon"] is String) {
            String iconPath = node.attributes["@icon"];
            if (!(iconPath.startsWith("http:") || iconPath.startsWith("https:"))) {
              RemoteRequester r = requester;
              BrokerNodeProvider np = r._linkManager.broker;
              if (np.iconOwnerMappings[iconPath] is! String) {
                np.iconOwnerMappings[iconPath] = r._linkManager.path;
              }
            }
          }
        } else {
          changes.add(name);
          if (removed) {
            node.children.remove(name);
          } else if (value is Map) {
            // TODO, also wait for children $is
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
