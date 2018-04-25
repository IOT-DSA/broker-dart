part of dsbroker.broker;

class PermissionPair {
  String group;
  int permission;
  bool isDefault;

  PermissionPair(this.group, this.permission) {
    isDefault = (group == 'default');
  }
}

abstract class BrokerNodePermission {
  List<PermissionPair> permissionList;

  BrokerNodePermission getPermissionChild(String str);

  void getPermission(
      Iterator<String> paths, List<String> groups, List<int> output) {
    // find permission for group
    if (permissionList != null) {
      int len = groups.length;
      for (int i = 0; i < len; ++i) {
        String group = groups[i];
        for (PermissionPair p in permissionList) {
          if (p.isDefault || p.group == group) {
            int permission = p.permission;
            output[i] = permission;
            if (permission == Permission.CONFIG) {
              // children won't overwrite a config permission
              return;
            }
            break;
          }
        }
      }
    }
    if (paths.moveNext()) {
      BrokerNodePermission child = getPermissionChild(paths.current);
      if (child != null) {
        child.getPermission(paths, groups, output);
      }
    }
  }

  // This is so that "default" is always evaluated last when
  // checking the permissions
  int _sortPermissionsByDefault(PermissionPair p1, PermissionPair p2) {
    if (p1.isDefault && p2.isDefault) {
      return 0;
    } else if (p1.isDefault) {
      return 1;
    } else if (p2.isDefault) {
      return -1;
    } else {
      return 0;
    }
  }

  void loadPermission(List l) {
    if (l != null && l.length > 0) {
      if (permissionList == null) {
        permissionList = new List<PermissionPair>();
      } else {
        permissionList.clear();
      }

      for (var pair in l) {
        if (pair is List &&
            pair.length == 2 &&
            pair[0] is String &&
            pair[1] is String) {
          String key = pair[0];
          String p = pair[1];
          int pint = Permission.parse(p);
          if (pint == Permission.NEVER) {
            // invalid permission
            continue;
          }
          permissionList.add(new PermissionPair(key, pint));
          permissionList.sort(_sortPermissionsByDefault);
        }
      }
      if (permissionList.isEmpty) {
        permissionList = null;
      }
    } else {
      permissionList = null;
    }
  }

  List serializePermission() {
    if (permissionList == null) {
      return null;
    }
    List rslt = [];
    if (permissionList != null) {
      for (var pair in permissionList) {
        rslt.add([pair.group, Permission.names[pair.permission]]);
      }
    }
    return rslt;
  }
}

class VirtualNodePermission extends BrokerNodePermission {
  Map<String, VirtualNodePermission> children =
      new Map<String, VirtualNodePermission>();

  BrokerNodePermission getPermissionChild(String str) {
    return children[str];
  }

  void load(Map m) {
    m.forEach((String name, Object value) {
      if (value is Map) {
        children[name] = new VirtualNodePermission()..load(value);
      }
    });
    if (m['?permissions'] is List) {
      loadPermission(m['?permissions']);
    }
  }

  Map serialize() {
    Map rslt = {};
    children.forEach((String name, VirtualNodePermission val) {
      rslt[name] = val.serialize();
    });
    List permissionData = this.serializePermission();
    if (permissionData != null) {
      rslt['?permissions'] = permissionData;
    }
    return rslt;
  }
}

class BrokerPermissions implements IPermissionManager {
  RootNode root;

  BrokerPermissions();

  int getPermission(String path, Responder resp) {
    if (root != null && root.permissionList != null) {
      List<int> output =
          new List<int>.filled(resp.groups.length, Permission.NONE);
      var iterator = path.split('/').iterator;
      // remove first ""
      iterator.moveNext();
      root.getPermission(iterator, resp.groups, output);
      int rslt = Permission.NONE;
      for (int p in output) {
        if (p > rslt) {
          rslt = p;
        }
      }
      if (rslt > resp.maxPermission) {
        return resp.maxPermission;
      }
      return rslt;
    }
    return resp.maxPermission;
  }
}
