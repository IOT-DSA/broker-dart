part of dsbroker.broker;

class GetPermissionAction extends BrokerStaticNode {
  GetPermissionAction(String path, BrokerNodeProvider provider) :
      super(path, provider) {
    configs[r"$name"] = "Get Permissions";
    configs[r"$invokable"] = "read";
    configs[r"$params"] = [
      {
        "name": "Path",
        "type": "string"
      }
    ];
    configs[r"$columns"] = [
      {"name": "permission", "type": "string"}
    ];
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {
    if (maxPermission == Permission.CONFIG && params != null
        && params["Path"] is String) {

      String path = params["Path"];
      int permission = provider.permissions.getPermission(path, responder);
      String output;
      if (permission == Permission.CONFIG) {
        LocalNode node = provider.getNode(path);
        if (node is BrokerNode) {
          List permission = node.serializePermission();
          if (permission != null) {
            output = JSON.encode(permission);
          }

        } else if (node is RemoteLinkNode) {
          BrokerNodePermission permissionChild = node
              ._linkManager
              .rootNode
              .getPermissionChildWithPath(node.remotePath, false);
          if (permissionChild != null) {
            List permission = permissionChild.serializePermission();
            if (permission != null) {
              output = JSON.encode(permission);
            }
          }
        }
      }
      return response..updateStream([[output]], streamStatus: StreamStatus.closed);
    }

    return response..close();
  }

  @override
  Map getSimpleMap() {
    var rslt = super.getSimpleMap();
    rslt[r"$hidden"] = true;
    return rslt;
  }
}


class UpdatePermissionAction extends BrokerStaticNode {
  UpdatePermissionAction(String path, BrokerNodeProvider provider) :
        super(path, provider) {
    configs[r"$name"] = "Update Permissions";
    configs[r"$invokable"] = "read";
    configs[r"$params"] = [
      {
        "name": "Path",
        "type": "string"
      },
      {
        "name": "Permissions",
        "type": "dynamic",
        "editor": "textarea"
      }
    ];
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
      InvokeResponse response, LocalNode parentNode,
      [int maxPermission = Permission.CONFIG]) {
    if (maxPermission == Permission.CONFIG && params != null
        && params["Path"] is String) {
      List permissions;
      if (params["Permissions"] is List) {
        permissions = params["Permissions"];
      } else if (params["Permissions"] is String) {
        try {
          permissions = JSON.decode(params["Permissions"]);
        } catch(err) {
          return response..close(DSError.INVALID_PARAMETER);
        }
      }
      String path = params["Path"];
      int permission = provider.permissions.getPermission(path, responder);
      if (permission == Permission.CONFIG) {
        if (path == '' || path == '/') {
          UpdateDefaultPermission.instance.updateData(permissions);
          provider.updateDefaultGroups(permissions);
          provider.updateConfigValue("defaultPermission", '', permissions, responder);
          return response..close();
        }
        if (permissions == null) {
          LocalNode node = provider.getNode(path);
          if (node is BrokerNode) {
            node.loadPermission(null);
          if (!node.persist()) {
              node.loadPermission(null);
              response.close(DSError.PERMISSION_DENIED);
            }
          } else if (node is RemoteLinkNode) {
            BrokerNodePermission permissionChild = node
                ._linkManager
                .rootNode
                .getPermissionChildWithPath(node.remotePath, false);
            if (permissionChild != null) {
              permissionChild.loadPermission(null);
              node._linkManager.rootNode.persist();
            }
          }
        } else {
          LocalNode node = provider.getOrCreateNode(path);
          if (node is BrokerNode) {
            node.loadPermission(permissions);
            if (!node.persist()) {
              node.loadPermission(null);
              response.close(DSError.PERMISSION_DENIED);
            }
          } else if (node is RemoteLinkNode) {
            BrokerNodePermission permissionChild = node
                ._linkManager
                .rootNode
                .getPermissionChildWithPath(node.remotePath, true);
            if (permissionChild != null) {
              permissionChild.loadPermission(permissions);
              node._linkManager.rootNode.persist();
            }
          }
        }
      }
    }

    return response..close();
  }

  @override
  Map getSimpleMap() {
    var rslt = super.getSimpleMap();
    rslt[r"$hidden"] = true;
    return rslt;
  }
}
