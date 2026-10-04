package org.flexlb.balance.scheduler;

import org.apache.commons.lang3.StringUtils;
import org.flexlb.balance.PlacementResult;
import org.flexlb.balance.strategy.CostBasedPrefillStrategy;
import org.flexlb.balance.strategy.DecodeSelector;
import org.flexlb.balance.strategy.RandomStrategy;
import org.flexlb.balance.strategy.SelectedRole;
import org.flexlb.config.ModelMetaConfig;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Failures;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class DefaultRouter {

    private final CostBasedPrefillStrategy prefillSelector;
    private final DecodeSelector decodeSelector;
    private final RandomStrategy vitSelector;
    private final List<RoleType> requiredRoles;

    @Autowired
    public DefaultRouter(
            CostBasedPrefillStrategy prefillSelector,
            DecodeSelector decodeSelector,
            RandomStrategy vitSelector,
            ModelMetaConfig modelMetaConfig) {
        this.prefillSelector = Objects.requireNonNull(
                prefillSelector, "prefillSelector");
        this.decodeSelector = Objects.requireNonNull(
                decodeSelector, "decodeSelector");
        this.vitSelector = Objects.requireNonNull(
                vitSelector, "vitSelector");
        this.requiredRoles = List.copyOf(
                Objects.requireNonNull(
                        modelMetaConfig, "modelMetaConfig").requiredRoles());
    }

    public PlacementResult<ProvisionalRoute, PlacementKey> select(BalanceContext context, String policyGroup) {
        if (context == null || context.getRequirements() == null) {
            Logger.error("masterRequest is null");
            return PlacementResult.rejected(Response.error(StrategyErrorType.INVALID_REQUEST));
        }
        RequestRequirements decodeAdmission = Objects.requireNonNull(context.getRequirements(), "registered request inputs");
        if (StringUtils.isNotBlank(policyGroup)) {
            Logger.info("Group routing policy selected group, requestId: {}, policy: {}, group: {}",
                    context.getRequestId(), "trafficPolicy", policyGroup);
        }
        String group = policyGroup;
        try (PinnedRouting routing = new PinnedRouting(new ArrayList<>(requiredRoles.size()))) {
            for (RoleType role : requiredRoles) {
                PlacementResult<SelectedRole, RoleType> result = selectRole(context, role, group, decodeAdmission);
                if (result.status() != PlacementResult.Status.SUCCESS) {
                    Logger.debug("Failed to select {} worker for request {}", role.getCode(), context.getRequestId());
                    return switch (result.status()) {
                        case REJECTED -> PlacementResult.rejected(result.failure(), result.diagnostics());
                        case BLOCKED -> PlacementResult.blocked(new PlacementKey(result.blocker(), group, null),
                                result.failure(), result.diagnostics());
                        default -> throw new IllegalStateException("unexpected selector result: " + result.status());
                    };
                }
                SelectedRole selection = result.value();
                try {
                    routing.selections().add(selection);
                } catch (RuntimeException | Error appendFailure) {
                    Failures.append(appendFailure, Failures.close(selection));
                    throw appendFailure;
                }
                if (StringUtils.isBlank(policyGroup)) {
                    group = selection.serverStatus().getGroup();
                }
            }
            var result = PlacementResult.<ProvisionalRoute, PlacementKey>success(ProvisionalRoute.prepare(
                    context, routing.selections(), buildSuccessResponse(routing.selections())));
            routing.selections().clear(); // The completed result now owns both stateful selections.
            return result;
        }
    }

    String resolvePolicyGroup(BalanceContext context) {
        if (context == null || context.getRequirements() == null) {
            return null;
        }
        var selector = context.getConfig().getRouter().getGroupSelector();
        RequestRequirements request = context.getRequirements();
        return selector == null ? null : selector.resolveTargetGroup(
                request.requestId(), request.apiKey(), request.seqLen()).orElse(null);
    }

    private PlacementResult<SelectedRole, RoleType> selectRole(
            BalanceContext context, RoleType role, String group, RequestRequirements decodeAdmission) {
        return switch (role) {
            case PREFILL, PDFUSION ->
                    prefillSelector.select(context, role, group);
            case DECODE -> decodeSelector.select(decodeAdmission, group);
            case VIT -> selectedOrBlocked(
                    vitSelector.select(context, role, group), role);
            case FRONTEND -> throw new IllegalArgumentException(
                    "Endpoint selection is not supported for FRONTEND");
        };
    }

    private static PlacementResult<SelectedRole, RoleType> selectedOrBlocked(
            SelectedRole selected, RoleType role) {
        return selected == null
                ? PlacementResult.blocked(role)
                : PlacementResult.success(selected);
    }

    private static Response buildSuccessResponse(List<SelectedRole> selections) {
        List<ServerStatus> statuses = new ArrayList<>(selections.size());
        for (SelectedRole selection : selections) {
            statuses.add(selection.serverStatus());
        }
        Response response = new Response();
        response.setSuccess(true);
        response.setServerStatus(statuses);
        return response;
    }

    /** Owns selected generation pins until ProvisionalRoute takes them or selection exits. */
    private record PinnedRouting(List<SelectedRole> selections) implements AutoCloseable {
        @Override
        public void close() {
            Throwable failure = null;
            for (int index = selections.size() - 1; index >= 0; index--) {
                failure = Failures.append(failure, Failures.close(selections.get(index)));
            }
            Failures.rethrow(failure, "route selection cleanup failed");
        }
    }
}
