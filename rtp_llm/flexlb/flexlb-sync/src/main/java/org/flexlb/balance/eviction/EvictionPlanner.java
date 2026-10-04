package org.flexlb.balance.eviction;

import org.flexlb.balance.endpoint.DecodeEndpoint.CapacityDeficit;
import org.flexlb.balance.endpoint.DecodeEndpoint.CapacityRelease;
import org.flexlb.balance.endpoint.DecodeEndpoint.DecodeRequestView;
import org.flexlb.balance.endpoint.DecodeEndpoint.ResourceSnapshot;
import org.flexlb.balance.scheduler.RequestRequirements;
import org.flexlb.config.PreemptionConfig;
import org.flexlb.config.VictimStage;
import org.flexlb.util.PriorityNormalizer;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Plans resource reclamation for the exact selected Decode endpoint. */
public final class EvictionPlanner {

    private EvictionPlanner() {
    }

    /** A decode plan never mixes Master-local removal with Engine Cancel. */
    private enum VictimOwnership {
        MASTER_LOCAL,
        ENGINE_CANCEL
    }

    // ==================== Decode reserved-only eviction (design doc 11-13) ====================

    /**
     * Candidate preference for slot eviction (design doc 11.3, first = evicted
     * first): priority asc → stage asc (reserved before accepted) →
     * requestId asc.
     */
    static final Comparator<DecodeRequestView> DECODE_SLOT_ORDER = Comparator
            .comparingInt(DecodeRequestView::priority)
            .thenComparingInt(v -> v.phase().ordinal())
            .thenComparingLong(DecodeRequestView::requestId);

    /**
     * Candidate preference for KV eviction (design doc 12.4): priority asc →
     * stage asc (reserved before accepted) → kvBucket desc (bigger
     * releases first, fewer victims) → requestId asc.
     */
    static final Comparator<DecodeRequestView> DECODE_KV_ORDER = Comparator
            .comparingInt(DecodeRequestView::priority)
            .thenComparingInt(v -> v.phase().ordinal())
            .thenComparing(v -> PriorityCostFunction.kvBucket(v.kvTokens()), Comparator.reverseOrder())
            .thenComparingLong(DecodeRequestView::requestId);

    /** The same capacity decision labels both ordinary admission and eviction planning. */
    public static String decodeEvictionCase(RequestRequirements request, ResourceSnapshot ep) {
        CapacityDeficit deficit = request.capacity().evaluate(
                ep.routing().placementUsage(), request.hardKvTokens(), request.expectedKvTokens());
        if (deficit.requests() > 0L && deficit.needsKv()) {
            return DecodeEvictionProposal.CASE_SLOT_AND_KV;
        }
        if (deficit.requests() > 0L) { return DecodeEvictionProposal.CASE_SLOT; }
        return deficit.needsKv() ? DecodeEvictionProposal.CASE_KV : null;
    }

    /**
     * Plan the cheapest decode eviction that clears the incoming request's
     * slot and/or KV deficit on the already selected endpoint.
     *
     * <p>Candidates are the strictly lower-priority reserved entries when
     * {@link VictimStage#DECODE_RESERVED} is allowed, plus — only when
     * {@link VictimStage#DECODE_ENGINE_OWNED} is allowed and the endpoint's
     * engine supports the Cancel RPC — the strictly lower-priority
     * engine-confirmed accepted/running entries. Running entries use a larger
     * stage cost, so an otherwise equivalent accepted-not-running victim is
     * preferred.
     *
     * @param request frozen incoming priority, demand and capacity policy
     * @param ep the selected Decode endpoint snapshot
     * @param preemption immutable policy for the current admission attempt
     * @param engineCancelSupported whether the selected endpoint supports Engine cancellation
     * @param failures out-param: per-endpoint infeasibility reason
     * @return the best proposal by {@link DecodeEvictionProposal#ORDER}, or
     *         {@code null} when this endpoint has no feasible plan
     */
    public static DecodeEvictionProposal planDecode(
            RequestRequirements request, ResourceSnapshot ep, PreemptionConfig preemption,
            boolean engineCancelSupported, Map<String, String> failures) {
        CapacityDeficit deficit = request.capacity().evaluate(
                ep.routing().placementUsage(), request.hardKvTokens(), request.expectedKvTokens());
        if (deficit.fits()) {
            failures.put(ep.routing().address(), "decode_capacity_sufficient");
            return null;
        }
        boolean localEvictionEnabled = preemption != null
                && preemption.allows(VictimStage.DECODE_RESERVED);
        boolean engineCancelEnabled = preemption != null
                && preemption.allows(VictimStage.DECODE_ENGINE_OWNED)
                && engineCancelSupported;
        DecodeEvictionProposal local = localEvictionEnabled
                ? planDecodeOneOwnership(request, ep, deficit,
                        VictimOwnership.MASTER_LOCAL, failures) : null;
        DecodeEvictionProposal engine = engineCancelEnabled
                ? planDecodeOneOwnership(request, ep, deficit,
                        VictimOwnership.ENGINE_CANCEL, failures) : null;
        if (local == null) { return engine; }
        if (engine == null) { return local; }
        return DecodeEvictionProposal.ORDER.compare(local, engine) <= 0 ? local : engine;
    }

    /** Try each needed dimension, then combine disjoint prefixes when neither suffices. */
    private static DecodeEvictionProposal planDecodeOneOwnership(
            RequestRequirements request, ResourceSnapshot ep, CapacityDeficit deficit,
            VictimOwnership ownership, Map<String, String> failures) {
        DecodeVictimSet slotOnly = deficit.requests() > 0L
                ? selectVictims(request, ep, CapacityRelease.NONE, Set.of(), ownership, false) : null;
        DecodeVictimSet kvOnly = deficit.needsKv()
                ? selectVictims(request, ep, CapacityRelease.NONE, Set.of(), ownership, true) : null;
        DecodeEvictionProposal slotSide = slotOnly != null && slotOnly.ok()
                && request.capacity().evaluate(ep.routing().placementUsage(), request.hardKvTokens(),
                        request.expectedKvTokens(), slotOnly.release()).fits()
                ? buildDecodeProposal(ep, DecodeEvictionProposal.CASE_SLOT, slotOnly.victims(),
                        slotOnly.harmProfile(), slotOnly.freedKvTokens()) : null;
        DecodeEvictionProposal kvSide = kvOnly != null && kvOnly.ok()
                && request.capacity().evaluate(ep.routing().placementUsage(), request.hardKvTokens(),
                        request.expectedKvTokens(), kvOnly.release()).fits()
                ? buildDecodeProposal(ep, DecodeEvictionProposal.CASE_KV, kvOnly.victims(),
                        kvOnly.harmProfile(), kvOnly.freedKvTokens()) : null;
        if (slotSide != null || kvSide != null) {
            if (slotSide == null) { return kvSide; }
            if (kvSide == null) { return slotSide; }
            return DecodeEvictionProposal.ORDER.compare(slotSide, kvSide) <= 0 ? slotSide : kvSide;
        }
        if (slotOnly == null || kvOnly == null) {
            DecodeVictimSet only = slotOnly == null ? kvOnly : slotOnly;
            failures.put(ep.routing().address(), only.ok() ? "insufficient_releasable_capacity" : only.failReason());
            return null;
        }

        double slotPressure = (double) deficit.requests() / Math.max(1L, request.capacity().maxEngineRequests());
        double kvPressure = (double) deficit.kvTokens() / Math.max(1L,
                request.capacity().kvBudget(ep.routing().placementUsage().totalKvTokens()));
        DecodeVictimSet first = kvPressure >= slotPressure ? kvOnly : slotOnly;
        if (!first.ok()) {
            failures.put(ep.routing().address(), first.failReason());
            return null;
        }
        DecodeVictimSet second = selectVictims(request,
                ep, first.release(), victimIds(first.victims()), ownership, kvPressure < slotPressure);
        if (!second.ok()) {
            failures.put(ep.routing().address(), second.failReason());
            return null;
        }
        CapacityRelease release = first.release().plus(second.release());
        if (!request.capacity().evaluate(ep.routing().placementUsage(),
                request.hardKvTokens(), request.expectedKvTokens(), release).fits()) {
            failures.put(ep.routing().address(), "insufficient_releasable_capacity");
            return null;
        }
        List<DecodeRequestView> victims = new ArrayList<>(first.victims());
        victims.addAll(second.victims());
        return buildDecodeProposal(ep, DecodeEvictionProposal.CASE_SLOT_AND_KV, victims,
                first.harmProfile().plus(second.harmProfile()),
                release.hardKvTokens());
    }

    /** Select an ordered prefix, stopping at the requested slot or hard/expected KV deficit. */
    private static DecodeVictimSet selectVictims(
            RequestRequirements request,
            ResourceSnapshot ep, CapacityRelease priorRelease,
            Set<Long> excludedVictimIds, VictimOwnership ownership, boolean reclaimKv) {
        List<DecodeRequestView> candidates = new ArrayList<>();
        for (DecodeRequestView entry : ep.requests().values()) {
            boolean ownershipMatches = ownership == VictimOwnership.MASTER_LOCAL
                    ? entry.phase().isMasterQueued() : entry.phase().requiresEngineCancel();
            if (ownershipMatches && !entry.claimedForPreemption()
                    && entry.priorityKnown() && PriorityNormalizer.hasPriority(entry.priority())
                    && entry.priority() < request.priority()
                    && !excludedVictimIds.contains(entry.requestId())
                    && (!reclaimKv || entry.expectedKvTokens() > 0L)) {
                candidates.add(entry);
            }
        }
        long requiredSlots = request.capacity().evaluate(
                ep.routing().placementUsage(), request.hardKvTokens(), request.expectedKvTokens(), priorRelease).requests();
        if (!reclaimKv && candidates.size() < requiredSlots) {
            return DecodeVictimSet.fail("insufficient_lower_priority_candidates");
        }
        candidates.sort(reclaimKv ? DECODE_KV_ORDER : DECODE_SLOT_ORDER);
        int limit = reclaimKv ? candidates.size() : (int) requiredSlots;
        long caseWeight = reclaimKv ? PriorityCostFunction.H_DECODE_KV_FULL
                : PriorityCostFunction.H_DECODE_SLOT_FULL;
        CapacityRelease release = CapacityRelease.NONE;
        PriorityHarmProfile.Builder harmProfile = PriorityHarmProfile.builder();
        int selected = 0;
        while (selected < limit) {
            if (reclaimKv && !request.capacity().evaluate(ep.routing().placementUsage(), request.hardKvTokens(),
                    request.expectedKvTokens(), priorRelease.plus(release)).needsKv()) { break; }
            DecodeRequestView victim = candidates.get(selected++);
            long stageCost = PriorityCostFunction.g(victim.phase());
            long lengthCost = reclaimKv ? PriorityCostFunction.lengthWasteCost(victim.kvTokens()) : 1L;
            harmProfile.add(victim.priority(), BigInteger.valueOf(caseWeight)
                    .multiply(BigInteger.valueOf(stageCost)).multiply(BigInteger.valueOf(lengthCost)));
            release = release.plus(victim.placementRelease());
        }
        if (reclaimKv && request.capacity().evaluate(ep.routing().placementUsage(), request.hardKvTokens(),
                request.expectedKvTokens(), priorRelease.plus(release)).needsKv()) {
            return DecodeVictimSet.fail("insufficient_releasable_kv");
        }
        return new DecodeVictimSet(candidates.subList(0, selected), harmProfile.build(),
                release, null);
    }

    private static Set<Long> victimIds(List<DecodeRequestView> victims) {
        Set<Long> ids = new java.util.HashSet<>(victims.size());
        for (DecodeRequestView victim : victims) {
            ids.add(victim.requestId());
        }
        return ids;
    }

    private static DecodeEvictionProposal buildDecodeProposal(ResourceSnapshot ep,
                                                              String evictionCase,
                                                              List<DecodeRequestView> victims,
                                                              PriorityHarmProfile harmProfile,
                                                              long freedKvTokens) {
        long tieBreak = Long.MAX_VALUE;
        for (DecodeRequestView victim : victims) {
            tieBreak = Math.min(tieBreak, victim.requestId());
        }
        return new DecodeEvictionProposal(ep.routing().address(),
                victims, evictionCase, freedKvTokens, harmProfile, tieBreak);
    }

    /**
     * Victim selection outcome for one dimension of a decode plan: either the
     * victims plus their h-weighted cost part, or a failure reason.
     */
    private record DecodeVictimSet(List<DecodeRequestView> victims,
                                   PriorityHarmProfile harmProfile,
                                   CapacityRelease release,
                                   String failReason) {

        static DecodeVictimSet fail(String reason) {
            return new DecodeVictimSet(null, PriorityHarmProfile.empty(), CapacityRelease.NONE, reason);
        }

        long freedKvTokens() { return release.hardKvTokens(); }

        boolean ok() {
            return failReason == null;
        }
    }
}
