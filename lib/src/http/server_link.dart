part of dsbroker.broker;

typedef Future<WebSocket> WebSocketUpgradeFunction(HttpRequest request,
  bool useStandardWebSocket, [
  protocolSelector(List<String> protocols)
]);
typedef void _WebSocketDisconnectCallback(HttpServerLink link);

/// a server link for both http and ws
class HttpServerLink implements ServerLink {
  static const bool ENABLE_MSGPACK = const bool.fromEnvironment(
    "dsa.msgpack.enabled",
    defaultValue: true
  );

  final String dsId;
  final String session;
  String token;
  final PublicKey publicKey;

  String path;
  String trustedTokenHash;
  String logName;

  _WebSocketDisconnectCallback onDisconnect;

  Completer<Requester> onRequesterReadyCompleter = new Completer<Requester>();

  Future<Requester> get onRequesterReady => onRequesterReadyCompleter.future;

  Requester requester;
  Responder responder;

  /// nonce for authentication, don't overwrite existing nonce
  ECDH tempNonce;

  /// nonce after user verified the public key
  ECDH verifiedNonce;

  ECDH get nonce => verifiedNonce;

  Connection connection;

  /// TODO(rinick): deprecate this, all dslinks need to support it
  final bool enableTimeout;
  /// TODO(rinick): deprecate this, all dslinks need to support it
  final bool enableAck;

  final List<String> _saltBases = new List<String>(3);
  final List<int> _saltInc = <int>[0, 0, 0];

  /// 3 salts, salt saltS saltL
  final List<String> salts = new List<String>(3);

  void _updateSalt(int type) {
    _saltInc[type] += DSRandom.instance.nextUint16();
    salts[type] = '${_saltBases[type]}${_saltInc[type].toRadixString(16)}';
  }

  HttpServerLink(String id, this.publicKey, ServerLinkManager linkManager,
      {NodeProvider nodeProvider,
      this.session, this.token, int maxPermission: Permission.CONFIG,
      this.enableTimeout: false, this.enableAck: true, this.onDisconnect, bool trusted: false})
      : dsId = id {
      path = linkManager.getLinkPath(id, token);
      if (path != null) {
        requester = linkManager.getRequester(id);
        if (nodeProvider != null){
          responder = linkManager.getResponder(id, nodeProvider, session, trusted);
          responder.maxPermission = maxPermission;
        }
      }

      for (int i = 0; i < 3; ++i) {
        List<int> bytes = new List<int>(12);
        for (int j = 0; j < 12; ++j) {
          bytes[j] = DSRandom.instance.nextUint8();
        }
        _saltBases[i] = Base64.encode(bytes);
        _updateSalt(i);
      }

    // TODO(rinick): need a requester ready property? because client can disconnect and reconnect and change isResponder value
  }

  /// check if public key matches the dsId
  bool get isDsIdValid {
    return publicKey.verifyDsId(dsId);
  }

  bool isRequester = false;
  bool enableStandardWebSocket;

  /// by default it's a responder only link
  bool isResponder = true;

  Map pendingLinkData;

  initLink(HttpRequest request, bool clientRequester, bool clientResponder,
      String serverDsId, String serverKey,
      {String wsUri: '/ws',
      String httpUri: '/http',
      int updateInterval: 200,
      Map linkData,
      List formats,
      bool trusted: false,
      bool enableCompression: false}) async {
    isRequester = clientResponder;
    isResponder = clientRequester;
    pendingLinkData = linkData;

    Map respJson = {
      "id": serverDsId,
      "publicKey": serverKey,
      "wsUri": wsUri,
      "httpUri": httpUri,
      "updateInterval": updateInterval,
      "version": DSA_VERSION
    };

    if (!trusted) {
      tempNonce = await ECDH.assign(publicKey, verifiedNonce);
      respJson["tempKey"] = tempNonce.encodedPublicKey;
      respJson["salt"] = salts[0];
      respJson["saltS"] = salts[1];
      respJson["saltL"] = salts[2];
    }

    if (requester is IRemoteRequester) {
      respJson["path"] = (requester as IRemoteRequester).responderPath;
    }

    if (formats != null) {
      if (formats.contains('msgpack') && ENABLE_MSGPACK) {
        respJson['format'] = 'msgpack';
      } else {
        respJson['format'] = 'json';
      }
    } else {
      respJson['format'] = 'json';
    }

    if (enableCompression == true) {
      enableStandardWebSocket = true;
    }

    {
      String msg = "Handshake Response: ${respJson}";
      String ln;

      if (logName != null) {
        ln = logName;
      } else if (path != null) {
        ln = "Link at ${path}";
      } else {
        ln = "Link ${dsId}";
      }

      logger.fine("[${ln}] ${msg}]");
    }

    updateResponseBeforeWrite(request);
    request.response.write(DsJson.encode(respJson));
    request.response.close();
  }

  bool verifySalt(int type, String hash) {
    if (hash == null) {
      logger.warning("$dsId was rejected due to an invalid auth value: null");
      return false;
    }
    if (verifiedNonce != null && verifiedNonce.verifySalt(salts[type], hash)) {
      _updateSalt(type);
      return true;
    } else if (tempNonce != null && tempNonce.verifySalt(salts[type], hash)) {
      _updateSalt(type);
      nonceChanged();
      return true;
    }
    if (tempNonce != null) {
      logger.warning("$dsId was rejected due to an invalid auth value: ${hash}, expects ${tempNonce.hashSalt(salts[type])}");
    } else if (verifiedNonce != null) {
      logger.warning("$dsId was unable to reconnect due to an invalid auth value: ${hash}, expects ${verifiedNonce.hashSalt(salts[type])}");
    } else {
      logger.warning("$dsId was rejected, handshake not initialized");
    }
    return false;
  }

  void nonceChanged() {
    verifiedNonce = tempNonce;
    tempNonce = null;
    if (connection != null) {
      connection.close();
      connection = null;
    }
  }

  WebSocketConnection wsconnection;

  void handleWsUpdate(HttpRequest request, bool trusted, [WebSocketUpgradeFunction upgrade]) {
    if (upgrade == null) {
      upgrade = (request, useCompression, [selector]) =>
        HttpHelper.upgradeToWebSocket(request, selector, useCompression);
    }

    if (!trusted && !verifySalt(0, request.uri.queryParameters['auth'])) {
      throw HttpStatus.UNAUTHORIZED;
    }

    if (wsconnection != null) {
      wsconnection.close();
    }
    updateResponseBeforeWrite(request, null, null, true);

    upgrade(request, enableStandardWebSocket, (m) => null).then((WebSocket websocket) {
      wsconnection = createWsConnection(
        websocket,
        request.uri.queryParameters['format']
      );

      wsconnection.addConnCommand('salt', salts[0]);
      if (connection != null) {
        connection.close();
      }
      connection = wsconnection;
      if (responder != null && isResponder) {
        responder.connection = connection.responderChannel;
      }

      if (requester != null && isRequester) {
        requester.connection = connection.requesterChannel;
        if (!onRequesterReadyCompleter.isCompleted) {
          onRequesterReadyCompleter.complete(requester);
        }
      }
    }).catchError((e, s) {
      logger.warning('Error upgrading websocket', e);
      logger.finest('Error upgrading websocket', e, s);
      try {
        if (e is WebSocketException) {
          request.response.statusCode = HttpStatus.BAD_REQUEST;
          request.response.writeln("Failed to upgrade to a WebSocket.");
        } else {
          request.response.statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
          request.response.writeln("Internal Server Error");
        }
      } catch (e) {}
      if (onDisconnect != null) {
        onDisconnect(this);
      }
      return request.response.close();
    });
  }

  void close() {
    if (wsconnection != null) {
      wsconnection.close();
      wsconnection = null;
      connection = null;
    }
  }

  WebSocketConnection createWsConnection(WebSocket websocket, String format) {
    var conn = new WebSocketConnection(
      websocket,
      enableTimeout: enableTimeout,
      enableAck: enableAck,
      useCodec: DsCodec.getCodec(format)
    );

    websocket.done.then((_) {
      close();
    });

    if (onDisconnect != null) {
      conn.onDisconnected.then((_) {
        onDisconnect(this);
        close();
      });
    }

    conn.printDisconnectedMessage = false;

    if (logName != null) {
      conn.logName = logName;
    } else if (path != null) {
      conn.logName = "Link at ${path}";
    } else {
      conn.logName = "Link ${dsId}";
    }
    return conn;
  }
}
