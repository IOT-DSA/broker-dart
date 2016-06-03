part of dsbroker.broker;

class AuthorizeDSLinkAction extends BrokerStaticNode {
  List dsidList = [];
  List groupList = [];
  List params;

  AuthorizeDSLinkAction(String path, BrokerNodeProvider provider) :
      super(path, provider) {
    configs[r"$name"] = "Authorize";

    params = [
      {
        "name": "DsId",
        "type": "enum",
      },
      {
        "name": "Group",
        "type": "string",
      },
      {
        "name": "Name",
        "type": "string"
      }
    ];
    configs[r"$invokable"] = "config";
    configs[r"$params"] = params;
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
    params[1] = {
      "name": "Group",
      "type": "string",
      "editor": "enum[${groups.join(',')}]"
    };
    updateList(r'$params');
  }

  void updateDsId(List dsids) {
    dsids.sort();
    params[0] = {
      "name": "DsId",
      "type": "enum[${dsids.join(',')}]"
    };
    updateList(r'$params');
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {

    if (params['DsId'] is String) {
      String dsId = params['DsId'];
      RemoteLinkNode node = provider.quarantineNode.children[dsId];
      if (node != null) {
        String group;
        String name;
        String connPath;
        if (params['Group'] is String) {
          group = params['Group'];
        }
        if (params['Name'] is String) {
          name = params['Name'];
        }
        if (name != null && name != '') {
          if (provider.quarantineNode.children.containsKey(name)) {
            return response..close(
              new DSError(
                "invalidParameter",
                msg: "name already exists"
              )
            );
          }

        }
        if (provider._links.containsKey(dsId)) {
          provider.removeLink(provider._links[dsId], dsId);
        }
        if (name != null && name != '') {
          connPath = provider.downstreamNameSS + name;
          provider._connPath2id[connPath] = dsId;
          provider._id2connPath[dsId] = connPath;
        } else {
          provider._id2connPath.remove(dsId);
          connPath = provider.makeConnPath(dsId, true);
        }

        if (group != null && group != '') {
          provider.getOrCreateNode(connPath, false).configs[r'$$group'] = group;
        }
      }
    } else {
      return response..close(DSError.INVALID_PARAMETER);
    }

    return response..close();
  }
}

class KickDSLinkAction extends BrokerStaticNode {
  List params;

  KickDSLinkAction(String path, BrokerNodeProvider provider) :
      super(path, provider) {
    configs[r"$name"] = "Deauthorize";
    configs[r"$invokable"] = "config";
    params = [
      {
        "name": "Name",
        "type": "enum"
      }
    ];
    configs[r'$params'] = params;
  }

  void updateNames(List names) {
    params[0] = {
      "name": "Name",
      "type": "enum[${names.join(',')}]"
    };
    updateList(r'$params');
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {
    if (params['Name'] is String) {
      String name = params['Name'];
      RemoteLinkNode node = provider.connsNode.children[name];
      if (node != null) {
        RemoteLinkManager manager = node._linkManager;
        String fullId = provider._connPath2id[manager.path];

        if (provider._links.containsKey(fullId)) {
          provider.removeLink(provider._links[fullId], fullId);
        }
        provider._connPath2id.remove(manager.path);
        provider._id2connPath.remove(fullId);
        provider.connsNode.children.remove(name);
        manager.inTree = false;

        provider.connsNode.updateList(name);

        DsTimer.timerOnceBefore(provider.saveConns, 300);
      }

    } else {
      return response..close(DSError.INVALID_PARAMETER);
    }

    return response..close();
  }
}

class UpdateGroupAction extends BrokerStaticNode {
  List params;

  UpdateGroupAction(String path, BrokerNodeProvider provider) :
      super(path, provider) {
    configs[r"$name"] = "Update Permission Group";
    configs[r"$invokable"] = "config";
    params = [
      {
        "name": "Name",
        "type": "enum"
      },
      {
        "name": "Group",
        "type": "group"
      },
    ];
    configs[r"$params"] = params;
  }

  void updateNames(List names) {
    params[0] = {
      "name": "Name",
      "type": "enum[${names.join(',')}]"
    };
    updateList(r'$params');
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
    params[1] = {
      "name": "Group",
      "type": "string",
      "editor": "enum[${groups.join(',')}]"
    };
    updateList(r'$params');
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {

    if (params['Name'] is String) {
      String name = params['Name'];
      RemoteLinkNode node = provider.connsNode.children[name];
      if (node != null) {
        String group;
        if (params['Group'] is String) {
          group = params['Group'];
        }
        node.configs[r'$$group'] = group;
        String fullId = provider._connPath2id[node.path];

        if (provider._links.containsKey(fullId)) {
          provider.removeLink(provider._links[fullId], fullId);
        }

        DsTimer.timerOnceBefore( provider.saveConns, 300);
      }

    } else {
      return response..close(DSError.INVALID_PARAMETER);
    }

    return response..close();
  }
}
