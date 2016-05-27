part of dsbroker.broker;

class BrokerStatsController {
  final BrokerNodeProvider provider;

  BrokerStatsController(this.provider);

  SimpleBrokerStatisticNode openRequestsNode;
  SimpleBrokerStatisticNode activeSubscriptionsNode;

  Disposable quarterSecondTicker;

  void init() {
    SimpleBrokerStatisticNode addNumberStat(String name, String id) {
      var node = new SimpleBrokerStatisticNode("/sys/${id}", provider);

      node.configs.addAll({
        r"$name": name,
        r"$type": "number"
      });
      provider.setNode(node.path, node);

      return node;
    }

    openRequestsNode = addNumberStat(
      "Open Requests",
      "openRequestsCount"
    );

    activeSubscriptionsNode = addNumberStat(
      "Active Subscriptions",
      "activeSubscriptionsCount"
    );

    start();
  }

  void start() {
    quarterSecondTicker = Scheduler.safeEvery(Interval.QUARTER_SECOND, () {
      updateQuarterSecondStats();
    });
  }

  void updateQuarterSecondStats() {
    if (openRequestsNode != null && openRequestsNode.hasSubscriber) {
      var count = provider.links.values.fold(
        0,
        (int a, BaseLink b) => a + b.responder.openResponseCount
      );

      openRequestsNode.updateValue(count);
    }

    if (activeSubscriptionsNode != null &&
      activeSubscriptionsNode.hasSubscriber) {
      var count = provider.links.values.fold(
        0,
        (int a, BaseLink b) => a + b.responder.subscriptionCount
      );

      activeSubscriptionsNode.updateValue(count);
    }
  }

  void stop() {
    if (quarterSecondTicker != null) {
      quarterSecondTicker.dispose();
      quarterSecondTicker = null;
    }
  }
}

class SimpleBrokerStatisticNode extends BrokerNode {
  SimpleBrokerStatisticNode(String path, BrokerNodeProvider provider)
    : super(path, provider);
}
