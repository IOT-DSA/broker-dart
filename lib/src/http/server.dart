part of dsbroker.broker;

class DsSimpleLinkManager implements ServerLinkManager {
  final Map<String, HttpServerLink> _links = new Map<String, HttpServerLink>();

  bool addLink(ServerLink link) {
    _links[link.dsId] = link;
    return true;
  }

  ServerLink getLinkAndConnectNode(String dsId, {String sessionId: ""}) {
    return _links[dsId];
  }

  void removeLink(ServerLink link, String id) {
    if (_links[id] == link) {
      _links.remove(id);
    }
  }

  Requester getRequester(String dsId) {
    return new Requester();
  }

  Responder getResponder(String dsId, NodeProvider nodeProvider,
      [String sessionId = "", bool trusted = false]) {
    return new Responder(nodeProvider);
  }

  void updateLinkData(String dsId, Map m) {
  }

  String getLinkPath(String dsId, String token) {
    return "/$dsId";
  }

  void onLinkDisconnected(ServerLink link) {

  }
}

class DsHttpServer {
  final NodeProvider nodeProvider;
  final ServerLinkManager _linkManager;

  String serverDsId;
  String serverPublicKey;
  PrivateKey privateKey;

  int updateInterval = 200;

  /// to open a secure server, SecureSocket.initialize() need to be called before start()
  DsHttpServer.start(dynamic address, {
    int httpPort: 8080,
    int httpsPort: 8443,
    sslContext: false,
    linkManager,
    bool shouldSaveFiles: true,
    this.privateKey,
    this.nodeProvider}) :
      _linkManager =
            (linkManager == null) ? new DsSimpleLinkManager() : linkManager {
    var completer = new Completer();
    onServerReady = completer.future;

    if (privateKey == null) {
      privateKey = loadBrokerPrivateKey(save: shouldSaveFiles);
    }

    serverPublicKey = privateKey.publicKey.qBase64;
    serverDsId = privateKey.publicKey.getDsId("broker-dsa");

    var futures = <Future>[];

    if (httpPort > 0) {
      futures.add(HttpServer.bind(address, httpPort).then((server) {
        logger.info("Listening on HTTP port $httpPort");
        server.listen(_handleRequest);
        httpServer = server;
      }).catchError((Object err) {
        logger.severe(err);
      }));
    }

    if (httpsPort > 0 && sslContext is SecurityContext) {
      futures.add(HttpServer.bindSecure(address, httpsPort, sslContext).then((HttpServer server) {
        logger.info("Listening on HTTPS port $httpsPort");
        server.listen(_handleRequest);
        httpsServer = server;
      }));
    }

    Future.wait(futures).then((_) {
      if (!completer.isCompleted) {
        completer.complete();
      }
    });
  }

  HttpServer httpServer;
  HttpServer httpsServer;
  Future onServerReady;

  Future stop() async {
    if (httpServer != null) {
      await httpServer.close();
    }

    if (httpsServer != null) {
      await httpsServer.close();
    }
  }

  void _handleRequest(HttpRequest request) {
    try {
      if (request.method == "HEAD" || request.method == "OPTIONS") {
        var response = request.response;

        if (!(const ["/conn", "/http", "/ws"].contains(request.uri.path)) && !request.uri.path.startsWith("/icon/")) {
          response.statusCode = HttpStatus.NOT_FOUND;
        }

        response.headers
            .set("Access-Control-Allow-Methods", "POST, OPTIONS, GET");
        response.headers.set("Access-Control-Allow-Headers", "Content-Type");

        String origin = request.headers.value("origin");

        if (request.headers.value("x-proxy-origin") != null) {
          origin = request.headers.value("x-proxy-origin");
        }

        if (origin == null) {
          origin = "*";
        }

        response.headers.set("Access-Control-Allow-Origin", origin);
        response.close();
        return;
      }

      if (!(const ["/conn", "/http", "/ws"].contains(request.uri.path)) && !request.uri.path.startsWith("/icon/")) {
        updateResponseBeforeWrite(request, HttpStatus.NOT_FOUND, null, true);
        request.response.statusCode = HttpStatus.NOT_FOUND;
        request.response.writeln("Not Found.");
        request.response.close();
        return;
      }

      if (request.uri.path.startsWith("/icon/")) {
        handleIcon(request);
        return;
      }

      String dsId = request.uri.queryParameters["dsId"];

      if (dsId == null || dsId.length < 43) {
        request.response.close();
        return;
      }

      switch (request.requestedUri.path) {
        case "/conn":
          _handleConn(request, dsId);
          break;
        case "/ws":
          _handleWsUpdate(request, dsId);
          break;
        default:
          request.response.close();
          break;
      }
    } catch (err) {
      if (err is int) {
        request.response.statusCode = err;
      }
      request.response.close();
    }
  }

  void _handleConn(HttpRequest request, String dsId) {
    String tokenHash = request.requestedUri.queryParameters["token"];
    bool trusted = false;

    if (tokenHash != null && nodeProvider is BrokerNodeProvider) {
      var tkn = tokenHash.substring(0, 16);
      trusted = (nodeProvider as BrokerNodeProvider)
        .tokenContext
        .trustedTokens
        .values
        .any((x) => x.id == tkn);
    }

    request.fold([], foldList).then((List<int> merged) {
      try {
        if (merged.length > 1024) {
          updateResponseBeforeWrite(request /*, HttpStatus.BAD_REQUEST*/);
          // invalid connection request
          request.response.close();
          return;
        } else if (merged.length == 0) {
          updateResponseBeforeWrite(request /*, HttpStatus.BAD_REQUEST*/);
          request.response.close();
          return;
        }
        String str = const Utf8Decoder().convert(merged);
        Map m = DsJson.decode(str);

        // validate the input structure
        if (m["linkData"] != null && m["linkData"] is! Map) {
          throw HttpStatus.BAD_REQUEST;
        }
        if (m["formats"] != null && m["formats"] is! List) {
          throw HttpStatus.BAD_REQUEST;
        }

        HttpServerLink link = _linkManager.getLinkAndConnectNode(dsId);

        if (link == null) {
          String publicKeyPointStr = m["publicKey"];
          var bytes = Base64.decode(publicKeyPointStr);
          if (bytes == null) {
            // public key is invalid
            throw HttpStatus.BAD_REQUEST;
          }

          link = new HttpServerLink(
            dsId,
            new PublicKey.fromBytes(bytes),
            _linkManager,
            token: tokenHash,
            nodeProvider: nodeProvider,
            enableTimeout: true,
            onDisconnect: onLinkDisconnected,
            trusted:trusted
          );

          if (trusted) {
            link.trustedTokenHash = tokenHash;
          } else if (!link.isDsIdValid) {
            // dsId doesn't match public key
            throw HttpStatus.BAD_REQUEST;
          }

          if (!_linkManager.addLink(link)) {
            throw HttpStatus.UNAUTHORIZED;
          }
        }

        link.initLink(
          request,
          m["isRequester"] == true,
          m["isResponder"] == true,
          serverDsId,
          serverPublicKey,
          updateInterval: updateInterval,
          linkData: m["linkData"],
          formats: m["formats"],
          trusted: trusted
        );
      } catch (err) {
        if (err is int) {
          // TODO: need protection because changing statusCode itself can throw
          updateResponseBeforeWrite(request, err);
        } else {
          updateResponseBeforeWrite(request);
        }
        request.response.close();
      }
    });
  }

  void _handleWsUpdate(HttpRequest request, String dsId) {
    HttpServerLink link = _linkManager.getLinkAndConnectNode(dsId);
    if (link != null) {
      bool trusted = link.trustedTokenHash != null &&
        request.requestedUri.queryParameters["token"] == link.trustedTokenHash;
      if (link.pendingLinkData != null) {
        _linkManager.updateLinkData(link.dsId, link.pendingLinkData);
        link.pendingLinkData = null;
      }
      link.handleWsUpdate(request, trusted);
    } else {
      throw HttpStatus.UNAUTHORIZED;
    }
  }

  void handleIcon(HttpRequest request) {
    var response = request.response;
    response.deadline = const Duration(seconds: 15);

    String path = request.uri.path;
    String name = path.substring(6);

    (nodeProvider as BrokerNodeProvider).getIconByName(name).then((ByteData data) {
      if (data == null) {
        response.statusCode = HttpStatus.NOT_FOUND;
        response.writeln("Icon Not Found.");
        response.close();
        return;
      }

      response.add(data.buffer.asUint8List(
        data.offsetInBytes,
        data.lengthInBytes
      ));
      response.close();
    }).catchError((e) {
      response.statusCode = HttpStatus.INTERNAL_SERVER_ERROR;
      response.close();
    });
  }

  void onLinkDisconnected(HttpServerLink link) {
  }
}
