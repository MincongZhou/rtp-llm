package org.flexlb.balance.scheduler;

import org.flexlb.balance.planner.GroupingPolicy;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Protect the migrated consumer boundaries, without constraining unrelated endpoint APIs. */
class SchedulerArchitectureTest {
    @Test
    void placementAndDeliveryConsumersCannotRetainOrAcceptTheWholeScheduler() {
        for (Class<?> consumer : new Class<?>[] {DirectRequestScheduler.class, QueuedRequestScheduler.class,
                RouteDeliveryStrategy.class, BatchDeliveryStrategy.class}) {
            assertNoDependency(consumer, RequestScheduler.class);
        }
        assertNoDependency(AbstractRequestScheduler.class, DefaultRouter.class);
    }

    @Test
    void groupingPoliciesAreStatelessAndPlacementDoesNotPretendToHaveACommonLifecycle() {
        assertTrue(Arrays.stream(GroupingPolicy.class.getDeclaredFields())
                .allMatch(f -> Modifier.isStatic(f.getModifiers())));
        assertTrue(RequestScheduler.class.isAssignableFrom(DirectRequestScheduler.class));
        assertTrue(RequestScheduler.class.isAssignableFrom(QueuedRequestScheduler.class));
        assertFalse(AutoCloseable.class.isAssignableFrom(DirectRequestScheduler.class));
    }

    @Test
    void publicContractHasOnlySchedulingAndCancellation() {
        org.junit.jupiter.api.Assertions.assertEquals(java.util.Set.of("submit", "cancel"),
                Arrays.stream(RequestScheduler.class.getDeclaredMethods()).map(java.lang.reflect.Method::getName)
                        .collect(java.util.stream.Collectors.toSet()));
        for (Class<?> type : new Class<?>[] {AbstractRequestScheduler.class, DirectRequestScheduler.class,
                QueuedRequestScheduler.class, AbstractRequestScheduler.class}) {
            assertFalse(Arrays.stream(type.getInterfaces()).anyMatch(i -> i.getSimpleName().equals("Control")));
        }
        assertFalse(Arrays.stream(AbstractRequestScheduler.class.getDeclaredMethods())
                .anyMatch(method -> java.util.Set.of("submit", "schedule", "commitDirectRoute", "enqueueRoute")
                        .contains(method.getName())));
    }

    private static void assertNoDependency(Class<?> consumer, Class<?> forbidden) {
        for (var field : consumer.getDeclaredFields()) {
            assertFalse(field.getType() == forbidden, field.toString());
        }
        for (var constructor : consumer.getDeclaredConstructors()) {
            assertFalse(Arrays.asList(constructor.getParameterTypes()).contains(forbidden), constructor.toString());
        }
        for (var method : consumer.getDeclaredMethods()) {
            assertFalse(method.getReturnType() == forbidden, method.toString());
            assertFalse(Arrays.asList(method.getParameterTypes()).contains(forbidden), method.toString());
        }
        for (Class<?> nested : consumer.getDeclaredClasses()) {
            assertNoDependency(nested, forbidden);
        }
    }
}
