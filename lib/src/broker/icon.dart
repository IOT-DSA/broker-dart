part of dsbroker.broker;

class BrokerSysGetIconNode extends BrokerNode {
  BrokerSysGetIconNode(String path, BrokerNodeProvider provider) : super(path, provider);

  @override
  InvokeResponse invoke(
    Map<String, dynamic> params,
    Responder responder,
    InvokeResponse response,
    Node parentNode, [int maxPermission = Permission.CONFIG]) {
    String name = params["Icon"];

    provider.getIconByName(name).then((ByteData data) {
      if (data != null) {
        response.updateStream(
          [
            [
              data
            ]
          ],
          columns: [
            {
              "name": "Data",
              "type": "binary"
            }
          ]
        );
      } else {
        return response..close();
      }
    }).catchError((e) {
      return response..close();
    });

    return response;
  }
}
