package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.prediction.DecodeCostFormula;
import org.flexlb.config.FlexlbConfig;

import java.util.Objects;
import java.util.List;

/** Immutable request input and demand captured once at registration; owns no resources. */
public record RequestRequirements(
        long requestId,
        int priority,
        long expectedKvTokens,
        DecodeEndpoint.AdmissionCapacity capacity,
        DecodeMode mode,
        DecodeCostFormula costFormula,
        long seqLen,
        String apiKey,
        List<Long> blockCacheKeys,
        long cacheKeyBlockSize,
        boolean requiresRouteReservation,
        int maxInflightBatchesPerPrefillWorker) {
    public RequestRequirements {
        blockCacheKeys = blockCacheKeys == null ? List.of() : List.copyOf(blockCacheKeys);
        Objects.requireNonNull(capacity, "capacity");
        Objects.requireNonNull(mode, "mode");
        Objects.requireNonNull(costFormula, "costFormula");
    }

    public long hardKvTokens() {
        return Math.max(0L, seqLen);
    }

    public static RequestRequirements capture(BalanceContext context) {
        var request = Objects.requireNonNull(context.getRequest(), "request");
        var config = Objects.requireNonNull(context.getConfig(), "request config");
        boolean routeDelivery = !config.getDispatcher().requiresGenerateInput();
        long seqLen = request.getSeqLen();
        long promptTokens = Math.max(0L, seqLen);
        long outputTokens = Math.max(0L, request.getMaxNewTokens());
        long expectedTokens = promptTokens > Long.MAX_VALUE - outputTokens
                ? Long.MAX_VALUE : promptTokens + outputTokens;
        var availability = config.getRouter().getRoles().getDecode().getAvailability();
        Long maxRequests = availability.getMaxEngineRequests();
        DecodeEndpoint.AdmissionCapacity capacity = new DecodeEndpoint.AdmissionCapacity(
                maxRequests == null ? 0L : maxRequests, availability.getMaxKvUsagePercent());
        return new RequestRequirements(context.getRequestId(), context.getPriority(),
                expectedTokens,
                capacity, DecodeMode.from(config),
                config.getRouter().getRoles().getDecode().getCostEstimator().compiledFormula(), seqLen, request.getApiKey(),
                request.getBlockCacheKeys(), request.getCacheKeyBlockSize(), routeDelivery,
                routeDelivery ? 0 : config.getDispatcher().getMaxInflightPerPrefillWorker());
    }

    public enum DecodeMode {
        IMMEDIATE,
        WAIT_AT_PLACEMENT,
        PREEMPT_AT_PLACEMENT;

        public static DecodeMode from(FlexlbConfig config) {
            if (config.isDirect()) { return IMMEDIATE; }
            return config.queueScheduler().getOrdering().preemptionPolicy().isPresent()
                    ? PREEMPT_AT_PLACEMENT : WAIT_AT_PLACEMENT;
        }
    }

}
