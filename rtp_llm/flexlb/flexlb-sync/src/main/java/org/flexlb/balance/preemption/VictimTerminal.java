package org.flexlb.balance.preemption;

import static com.google.common.base.Preconditions.checkArgument;

/** Authoritative terminal proof for one exact preemption victim. */
public record VictimTerminal(long requestId) {

    public VictimTerminal {
        checkArgument(requestId > 0, "requestId must be positive");
    }
}
