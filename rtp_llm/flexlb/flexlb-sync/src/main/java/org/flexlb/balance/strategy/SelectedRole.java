package org.flexlb.balance.strategy;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Failures;

import java.util.Objects;

/**
 * One exact endpoint-generation selection.
 *
 * <p>The selection owns its generation pin and is transferred as a whole to
 * route admission. It carries only immutable
 * routing output besides that pin; {@link ServerStatus} remains response
 * metadata and is never an ownership token.</p>
 */
public final class SelectedRole implements AutoCloseable {

    private enum Owner { SELECTOR, ROUTE, CLOSED }

    private final WorkerEndpoint.GenerationPin generationPin;
    private volatile Owner owner = Owner.SELECTOR;
    private final ServerStatus serverStatus;
    private final long prefillWorkMs;
    private final long placementVersion;

    private SelectedRole(
            WorkerEndpoint.GenerationPin generationPin,
            ServerStatus serverStatus,
            long prefillWorkMs,
            long placementVersion) {
        this.generationPin = generationPin;
        this.serverStatus = serverStatus;
        if (!serverStatus.isSuccess()) {
            throw new IllegalArgumentException(
                    "SelectedRole requires successful response metadata");
        }
        WorkerEndpoint endpoint = generationPin.endpoint();
        if (!Objects.equals(serverStatus.getServerIp(), endpoint.getIp())
                || serverStatus.getHttpPort() != endpoint.getHttpPort()) {
            throw new IllegalArgumentException(
                    "selection metadata does not match pinned endpoint address");
        }
        if (prefillWorkMs >= 0L
                && (!(endpoint instanceof PrefillEndpoint)
                        || serverStatus.getRole() != RoleType.PREFILL
                                && serverStatus.getRole() != RoleType.PDFUSION)) {
            throw new IllegalArgumentException(
                    "Prefill selection requires a Prefill endpoint role");
        }
        if (serverStatus.getRole() == RoleType.DECODE
                && !(endpoint instanceof DecodeEndpoint)) {
            throw new IllegalArgumentException(
                    "Decode selection requires a Decode endpoint role");
        }
        this.prefillWorkMs = prefillWorkMs;
        if (placementVersion < 0L) {
            throw new IllegalArgumentException(
                    "placementVersion must be non-negative");
        }
        this.placementVersion = placementVersion;
    }

    public static SelectedRole prefill(
            WorkerEndpoint.GenerationPin generationPin,
            ServerStatus serverStatus,
            long prefillWorkMs,
            long placementVersion) {
        if (prefillWorkMs < 0L) {
            try (generationPin) {
                throw new IllegalArgumentException("Prefill work must be non-negative");
            }
        }
        return createOwned(
                generationPin, serverStatus, prefillWorkMs,
                placementVersion);
    }

    public static SelectedRole decode(
            WorkerEndpoint.GenerationPin generationPin,
            ServerStatus serverStatus,
            long placementVersion) {
        if (serverStatus == null || serverStatus.getRole() != RoleType.DECODE) {
            try (generationPin) {
                throw new IllegalArgumentException("Decode selection requires Decode metadata");
            }
        }
        return createOwned(generationPin, serverStatus, -1L, placementVersion);
    }

    public static SelectedRole stateless(
            WorkerEndpoint.GenerationPin generationPin,
            ServerStatus serverStatus) {
        return createOwned(
                generationPin, serverStatus, -1L, 0L);
    }

    /** Calling a factory consumes the pin, including every validation failure. */
    private static SelectedRole createOwned(
            WorkerEndpoint.GenerationPin generationPin,
            ServerStatus serverStatus,
            long prefillWorkMs,
            long placementVersion) {
        Throwable failure = null;
        try {
            return new SelectedRole(generationPin, serverStatus, prefillWorkMs, placementVersion);
        } catch (RuntimeException | Error constructionFailure) {
            failure = constructionFailure;
            throw constructionFailure;
        } finally {
            if (failure != null && generationPin != null) {
                Failures.append(failure, Failures.close(generationPin));
            }
        }
    }

    public ServerStatus serverStatus() {
        return serverStatus;
    }

    public long prefillWorkMs() {
        if (prefillWorkMs < 0L) {
            throw new IllegalStateException(
                    "selection does not carry Prefill work");
        }
        return prefillWorkMs;
    }

    public long placementVersion() {
        return placementVersion;
    }

    /** Transfer the whole selected result; its routing facts stay immutable. */
    public synchronized void transferToRoute() {
        if (owner != Owner.SELECTOR) {
            throw new IllegalStateException("selected endpoint generation was already consumed");
        }
        owner = Owner.ROUTE;
    }

    public WorkerEndpoint endpoint() {
        return generationPin.endpoint();
    }

    public WorkerEndpoint.GenerationPin generationPin() {
        if (owner == Owner.CLOSED) {
            throw new IllegalStateException("selected endpoint generation is closed");
        }
        return generationPin;
    }

    @Override
    public void close() {
        synchronized (this) {
            if (owner == Owner.CLOSED) {
                return;
            }
            owner = Owner.CLOSED;
        }
        generationPin.close();
    }
}
