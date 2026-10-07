package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.BalanceContext;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.util.CommonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkArgument;

/** Random selection for the stateless VIT role. */
@Component
public final class RandomStrategy {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RandomStrategy.class);

    private final EndpointRegistry endpointRegistry;

    public RandomStrategy(EndpointRegistry endpointRegistry) {
        this.endpointRegistry = endpointRegistry;
    }

    public SelectedRole select(
            BalanceContext context, RoleType role, String group) {
        checkArgument(role == RoleType.VIT, "RANDOM endpoint selection is supported only for VIT");
        List<String> addresses =
                endpointRegistry.endpointAddressSnapshot(RoleType.VIT);
        if (addresses.isEmpty()) {
            return null;
        }

        int start = ThreadLocalRandom.current().nextInt(addresses.size());
        SelectedRole selected = capture(addresses.get(start), group, context.getRequestId());
        if (selected != null) {
            return selected;
        }
        // Randomize only after a rejected address; a sequential fallback biases group selection.
        List<String> remaining = new ArrayList<>(addresses);
        remaining.remove(start);
        Collections.shuffle(remaining, ThreadLocalRandom.current());
        for (String address : remaining) {
            selected = capture(address, group, context.getRequestId());
            if (selected != null) {
                return selected;
            }
        }
        LOGGER.warn(
                "No VIT worker available out of {} registered workers",
                addresses.size());
        return null;
    }

    private SelectedRole capture(String address, String group, long requestId) {
        WorkerEndpoint.GenerationPin pin = endpointRegistry.capture(RoleType.VIT, address);
        if (pin == null) {
            return null;
        }
        try {
            WorkerStatus status = pin.endpoint().getStatus();
            WorkerStatus.TopologySnapshot topology = status.topologySnapshot();
            if (group != null && !group.equals(topology.group())) {
                return null;
            }
            WorkerStatus.EngineObservation engine = status.committedEngineObservation();
            ServerStatus result = new ServerStatus();
            result.setSuccess(true);
            result.setRole(RoleType.VIT);
            result.setRequestId(requestId);
            result.setGroup(topology.group());
            result.setServerIp(topology.ip());
            result.setHttpPort(topology.port());
            result.setGrpcPort(CommonUtils.toGrpcPort(topology.port()));
            result.setDpRank(engine.dpRank());

            SelectedRole selected = SelectedRole.stateless(pin, result);
            pin = null;
            return selected;
        } finally {
            if (pin != null) {
                pin.close();
            }
        }
    }
}
