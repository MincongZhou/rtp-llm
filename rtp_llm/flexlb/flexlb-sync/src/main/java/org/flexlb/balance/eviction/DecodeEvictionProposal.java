package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeResources.DecodeRequestView;

import java.util.Comparator;
import java.util.List;

/**
 * Pure planning result for one decode eviction on one endpoint
 * (design doc 11-13). Produced by {@link EvictionPlanner#planDecode}; carries
 * no live endpoint reference and has zero side effects.
 *
 * <p>Victims are strictly lower priority than the incoming request. Depending
 * on the feature gate, they may be Master shadow reservations or
 * engine-confirmed accepted/running requests; confirmed victims require the
 * Cancel-and-release-confirm commit path.
 *
 * @param endpointId       decode endpoint key ("ip:httpPort")
 * @param victims          selected victims in eviction order
 * @param evictionCase     {@link #CASE_SLOT} / {@link #CASE_KV} / {@link #CASE_SLOT_AND_KV}
 * @param freedKvTokens    sum of the victims' releasable hard KV tokens
 * @param priorityHarmProfile exact priority harm, the absolute first comparison dimension
 * @param deterministicTieBreak smallest victim request id
 */
public record DecodeEvictionProposal(
        String endpointId,
        List<DecodeRequestView> victims,
        String evictionCase,
        long freedKvTokens,
        PriorityHarmProfile priorityHarmProfile,
        long deterministicTieBreak) {

    public DecodeEvictionProposal {
        victims = List.copyOf(victims);
        boolean hasLocal = victims.stream().anyMatch(victim -> victim.phase().isMasterQueued());
        boolean hasCancel = victims.stream().anyMatch(victim -> victim.phase().requiresEngineCancel());
        if (hasLocal && hasCancel) {
            throw new IllegalArgumentException(
                    "decode proposal cannot mix Master-local and Engine-Cancel victims");
        }
    }

    public boolean requiresEngineCancel() {
        return !victims.isEmpty() && victims.get(0).phase().requiresEngineCancel();
    }

    /** Concurrency slots exhausted (design doc 11). */
    public static final String CASE_SLOT = "decode_slot_full";
    /** Real KV available below the incoming hard demand (design doc 12). */
    public static final String CASE_KV = "decode_kv_full";
    /** Both deficits at once (design doc 13). */
    public static final String CASE_SLOT_AND_KV = "decode_slot_and_kv_full";

    /**
     * Prefer less exact priority harm, then fewer victims and the smallest request id.
     * Scalar diagnostic cost never participates in priority-safety ordering.
     */
    public static final Comparator<DecodeEvictionProposal> ORDER = Comparator
            .comparing(DecodeEvictionProposal::priorityHarmProfile)
            .thenComparingInt(proposal -> proposal.victims().size())
            .thenComparingLong(DecodeEvictionProposal::deterministicTieBreak)
            .thenComparing(DecodeEvictionProposal::endpointId);
}
