part of dsbroker.broker;

class UpdateDefaultPermission extends BrokerStaticNode {
  static UpdateDefaultPermission instance;

  List params;

  UpdateDefaultPermission(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    instance = this;
    params = [
      {
        "name": "Data",
        "type": "string",
        "editor": "textarea",
        "default": "[\n]"
      }
    ];
    configs[r"$name"] = "Update Default Permission";
    configs[r"$invokable"] = "config";
    configs[r'$params'] = params;
  }

  void updateData(List defaultPermission) {
    if (defaultPermission == null) {
      return;
    }
    StringBuffer sb = new StringBuffer();
    sb.write('[\n');
    bool first = true;
    if (defaultPermission != null) {
      for (List row in defaultPermission) {
        if (first) {
          first = false;
          sb.write('  ["${row[0]}","${row[1]}"]');
        } else {
          sb.write(',\n  ["${row[0]}","${row[1]}"]');
        }
      }
    }
    sb.write('\n]');
    params[0] = {
      "name": "Data",
      "type": "string",
      "editor": "textarea",
      "default": sb.toString()
    };
    updateList(r'$params');
  }

  @override
  InvokeResponse invoke(Map params, Responder responder,
    InvokeResponse response, LocalNode parentNode,
    [int maxPermission = Permission.CONFIG]) {
    if (params['Data'] is String) {
      String data = params['Data'];
      List json;
      try {
        json = JSON.decode(data);
        BrokerNodeProvider.fixPermissionList(json);
      } catch (err) {
        return response..close(DSError.INVALID_PARAMETER);
      }
      updateData(json);
      provider.updateDefaultGroups(json);
      provider.updateConfigValue("defaultPermission", json, responder);
    } else {
      return response..close(DSError.INVALID_PARAMETER);
    }

    return response..close();
  }
}
class AllowAllLinksNode extends BrokerStaticNode {
  AllowAllLinksNode(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    configs[r"$name"] = "Allow All Links";
    configs[r"$writable"] = "config";
    configs[r"$type"] = "bool";
    updateValue(provider.acceptAllConns);
  }

  Response setValue(Object value, Responder responder, Response response,
    [int maxPermission = Permission.CONFIG]) {
    if (value is! bool) {
      return response..close(DSError.INVALID_PARAMETER);
    }

    if (value != provider.acceptAllConns) {
      provider.acceptAllConns = value;
      provider.updateConfigValue("allowAllLinks", value, responder);
    }

    return super.setValue(value, responder, response, maxPermission);
  }
}

class EnableQuarantineNode extends BrokerStaticNode {
  EnableQuarantineNode(String path, BrokerNodeProvider provider)
    : super(path, provider) {
    configs[r"$name"] = "Enable Quarantine";
    configs[r"$writable"] = "config";
    configs[r"$type"] = "bool";
    updateValue(provider.enabledQuarantine);
  }

  Response setValue(Object value, Responder responder, Response response,
    [int maxPermission = Permission.CONFIG]) {
    if (value is! bool) {
      return response..close(DSError.INVALID_PARAMETER);
    }

    if (value != provider.enabledQuarantine) {
      provider.enabledQuarantine = value;
      provider.updateConfigValue("quarantine", value, responder);
    }

    return super.setValue(value, responder, response, maxPermission);
  }
}
