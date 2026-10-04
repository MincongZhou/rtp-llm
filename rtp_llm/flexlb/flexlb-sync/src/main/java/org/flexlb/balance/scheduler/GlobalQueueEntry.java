package org.flexlb.balance.scheduler;


/** One request identity retained by the model-wide ordered queue. */
final class GlobalQueueEntry {

    final BalanceContext context;
    final int priority;
    final String routingGroup;
    long sequence;
    volatile boolean removed = true;
    GlobalQueueEntry previous;
    GlobalQueueEntry next;

    GlobalQueueEntry(BalanceContext context,
            int priority, String routingGroup) {
        this.context = context;
        this.priority = priority;
        this.routingGroup = routingGroup;
    }
}
