# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Module Overview

flexlb-sync is the core load balancing module of FlexLB. It handles:
- Full-fleet Prefill and Decode cost selection and VIT random selection
- Worker node status synchronization via gRPC
- Master election using ZooKeeper
- Request routing across different role types (PREFILL, DECODE, PDFUSION, VIT)
- Integration with flexlb-cache for KV cache management

## Key Architecture Concepts

### Routing and scheduling
- `DefaultRouter` performs one multi-role selection and returns exact endpoint-generation capabilities
- `RequestScheduler` exposes submit, cancel, stopAccepting, and termination for both modes
- `AbstractRequestScheduler` executes protocol effects outside the request lock and tracks all generation obligations
- `DirectRequestScheduler` accepts requests and owns immediate selection and commit
- `QueuedRequestScheduler` accepts requests and owns ordering, planning, capacity waits, and commit
- `PlacementConfiguration` constructs the fixed-mode scheduler at startup
- `RequestRepository` owns shared registration, identity lookup, and archival; each request retains its original Scheduler
- `WorkerBatcher` owns the endpoint queue and delivery runtime; stateless `GroupingPolicy` choices select SINGLE/FIXED_WINDOW groups for both live scheduling and projection
- `DeliveryStrategy` implementations choose NON_BATCH or BATCH delivery without reselecting endpoints, using the exact request owner and its `BalanceContext.DeliveryClaim`

### Role-Based Routing
The system routes requests through multiple worker types based on model requirements:
- **PREFILL**: Initial token processing
- **DECODE**: Token generation
- **PDFUSION**: Prefill-Decode fusion workers
- **VIT**: Vision-language model processing

Key classes:
- `RoleType` (in flexlb-common): Enum defining worker roles
- `DefaultRouter`: Implements multi-role routing with rollback support
- `BalanceContext`: Owns private request lifecycle and exact admission, cleanup, deadline, and preemption participation state; behavior methods preserve the request monitor contract
- `RequestRepository`: Owns shared request identities and exact archival only
- `SchedulerRuntime`: Creates one startup scheduler and owns maintenance and ordered shutdown; `ExpirationTimer` only schedules and drains exact registrations

### Worker Status Synchronization
- `GrpcWorkerStatusRunner`: Periodically fetches worker status via gRPC
- `EndpointRegistry`: Owns discovered generations, endpoint publication/retirement and immutable routing snapshots
- `GrpcCacheStatusCheckRunner`: Syncs KV cache status with flexlb-cache module

### Master Election
- `ZookeeperMasterElectService`: ZooKeeper-based leader election
- `LBStatusConsistencyService`: Handles master-slave consistency

## Development Commands

### Build this module only
```bash
# From flexlb-sync directory
mvn clean package -DskipTests

# From parent directory
mvn clean package -pl flexlb-sync -DskipTests
```

### Run tests
```bash
# Run all tests in this module
mvn test

# Run a specific test class
mvn test -Dtest=DefaultRouterTest

# Run a specific test method
mvn test -Dtest=DefaultRouterTest#testRouteSuccess
```

### Code formatting check
```bash
# From parent directory
mvn spotless:check -Pspotless-check

# Auto-format code
mvn spotless:apply -Pspotless-check
```

## Project Structure

```
flexlb-sync/
├── balance/
│   ├── endpoint/                  # exact generation ownership and capacity
│   ├── eviction/                  # priority-only exact-route preemption
│   ├── planner/                   # endpoint-local decision-group formation
│   ├── prediction/                # Prefill execution-time estimators
│   ├── projection/                # frozen queue/TTFT projections
│   ├── scheduler/
│   │   ├── DefaultRouter.java       # one-pass multi-role routing
│   │   ├── DirectRequestScheduler.java # immediate selection/commit
│   │   ├── QueuedRequestScheduler.java # model-wide ordering/commit
│   │   ├── BalanceContext.java      # request lifecycle protocol
│   │   ├── RequestScheduler.java    # public scheduling and drain contract
│   │   ├── RequestRepository.java   # exact shared registration and archival
│   │   ├── SchedulerRuntime.java    # maintenance and shutdown
│   │   └── WorkerBatcher.java       # endpoint decision/delivery runtime
│   └── strategy/
│       ├── RandomStrategy.java             # VIT selection
│       ├── CostBasedPrefillStrategy.java   # Predicted Prefill cost strategy
│       └── DecodeSelector.java    # Capacity-filtered Decode cost selection
├── consistency/
│   ├── MasterElectService.java      # Master election interface
│   └── ZookeeperMasterElectService.java  # ZK implementation
├── sync/
│   ├── runner/
│   │   ├── GrpcWorkerStatusRunner.java    # Worker status sync
│   │   └── GrpcCacheStatusCheckRunner.java # Cache status sync
└── service/
    └── grpc/
        └── EngineGrpcService.java   # gRPC client for workers
```

## Key Dependencies

This module depends on:
- **flexlb-common**: Shared data models (`ServerStatus`, `RoleType`, `WorkerStatus`); `BalanceContext` belongs to flexlb-sync
- **flexlb-cache**: KV cache management (`CacheAwareService`, `KvCacheManager`)
- **flexlb-grpc**: gRPC protocol definitions and clients

External dependencies:
- Spring Boot 2.7.18 (WebFlux for reactive programming)
- Apache Curator 5.4.0 (ZooKeeper client)
- gRPC 1.65.0
- Caffeine (local caching)
- OpenTelemetry (distributed tracing)

## Important Implementation Notes

### Rollback Mechanism
When routing fails for a later role type (e.g., PREFILL succeeds but DECODE fails),
`DefaultRouter` closes the exact `SelectedRole` capabilities already selected and
rolls back any direct-placement endpoint reservations.

### Load Balancer Selection
The router calls explicit role selectors. Prefill and Decode selectors must
evaluate the complete live fleet before reducing to the configured policy
winner. Capacity commit, delivery, and priority preemption must consume that
winner rather than invoke another selector.

### Worker Status Updates
Worker status is updated asynchronously by scheduled runners. `EndpointRegistry`
owns discovered worker identities and generation-fenced endpoint publication,
and exposes immutable routing snapshots and exact captures to strategies.

### Cache Integration
The module calls `CacheAwareService` from flexlb-cache to update cache information
received from workers. See `GrpcCacheStatusCheckRunner`.

## Configuration

This module reads configuration from:
- `FLEXLB_CONFIG`: Load balance strategy, timeouts, batch settings
- `FLEXLB_SYNC_CONSISTENCY_CONFIG`: ZooKeeper connection, master election
- `MODEL_SERVICE_CONFIG`: Backend worker endpoints

Configuration is injected via `ConfigService` interface (implementation in flexlb-common).

## Testing Strategy

- Unit tests use Mockito 5.20.0 (no PowerMock needed with Java 21)
- Test classes mirror source structure (e.g., `DefaultRouterTest` for `DefaultRouter`)
- Mock external dependencies (gRPC clients, cache managers, config service)
- Focus on routing logic, strategy selection, and error handling

## Important Reminders
Do what is asked; no more, no less.
Always prefer editing existing files over creating new ones.
Do not proactively create documentation files (*.md) or README files unless explicitly requested.

Spring creates one scheduler at startup. The scheduler and request contexts use the ConfigService startup configuration directly; changes to scheduling and shared WorkerBatcher settings require restart. BATCH abandonment requires sender exit and exact remote cleanup proof before archival; response completion does not release these obligations. Ordinary cancellation capability is checked before using the Engine cleanup protocol.
