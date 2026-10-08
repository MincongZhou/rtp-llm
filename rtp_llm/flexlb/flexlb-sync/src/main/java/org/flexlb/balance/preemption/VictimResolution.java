package org.flexlb.balance.preemption;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Request-side resolution of one exact preemption victim: it finalized or resumed delivery.
 * Decode release is established separately by the endpoint ledger.
 */
public record VictimResolution(long requestId) {

    public VictimResolution {
        checkArgument(requestId > 0, "requestId must be positive");
    }
}
