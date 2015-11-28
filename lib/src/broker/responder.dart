part of dsbroker.broker;

class BrokerResponder extends Responder {
  BrokerResponder(NodeProvider nodeProvider, String reqId) : super(nodeProvider, reqId);

  void invoke(Map m) {
    Path path = Path.getValidNodePath(m['path']);
    if (path != null && path.isAbsolute) {
      int rid = m['rid'];
      LocalNode parentNode = nodeProvider.getOrCreateNode(path.parentPath, false);
      LocalNode actionNode;
      bool doublePermissionCheck = false;
      if (path.name == 'getHistory' && parentNode.attributes['@@getHistory'] is Map) {
        // alias node for getHistory action
        // TODO, should we make this a generic way of alias node
        Map m  = parentNode.attributes['@@getHistory'];
        if (m['val'] is List && (m['val'] as List).length > 0) {
          String path = m['val'][0];
          actionNode = nodeProvider.getOrCreateNode(path, false);
          doublePermissionCheck = true;
        } else {
          actionNode = parentNode.getChild(path.name);
        }
      } else {
        actionNode = parentNode.getChild(path.name);
      }

      if (actionNode == null) {
        closeResponse(m['rid'], error: DSError.PERMISSION_DENIED);
        return;
      }
      int permission = nodeProvider.permissions.getPermission(path.path, this);
      int maxPermit = Permission.parse(m['permit']);
      if (maxPermit < permission) {
        permission = maxPermit;
      }

      if (doublePermissionCheck) {
        int permission2 = nodeProvider.permissions.getPermission(actionNode.path, this);
        if (permission2 < permission2) {
          permission = permission2;
        }
      }

      if (actionNode.getInvokePermission() <= permission) {
        actionNode.invoke(m['params'], this,
            addResponse(new InvokeResponse(this, rid, parentNode, actionNode, path.name)), parentNode,
            permission);
      } else {
        closeResponse(m['rid'], error: DSError.PERMISSION_DENIED);
      }
    } else {
      closeResponse(m['rid'], error: DSError.INVALID_PATH);
    }
  }
}
