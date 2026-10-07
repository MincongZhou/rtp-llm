package org.flexlb.consistency;

import org.flexlb.domain.consistency.LBConsistencyConfig;
import org.flexlb.domain.consistency.MasterChangeNotifyReq;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.mock.env.MockEnvironment;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LBStatusConsistencyLifecycleTest {

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void contextShutdownClosesOnlyEnabledElection(boolean enabled) {
        var election = mock(ZookeeperMasterElectService.class);
        var config = new LBConsistencyConfig();
        config.setNeedConsistency(enabled);
        when(election.getLbConsistencyConfig()).thenReturn(config);
        try (var context = new AnnotationConfigApplicationContext()) {
            context.registerBean(LBStatusConsistencyService.class,
                    () -> new LBStatusConsistencyService(election, new MockEnvironment()));
            context.refresh();
            verify(election, times(0)).destroy();
        }
        verify(election, times(enabled ? 1 : 0)).destroy();
    }
    @ParameterizedTest
    @CsvSource({"true,true", "true,false", "false,true", "false,false"})
    void masterChangeRefreshesOnlyAnEnabledMatchingRole(boolean enabled, boolean matchingRole) {
        var election = mock(ZookeeperMasterElectService.class);
        var config = new LBConsistencyConfig();
        config.setNeedConsistency(enabled);
        when(election.getLbConsistencyConfig()).thenReturn(config);
        var service = new LBStatusConsistencyService(election, new MockEnvironment());
        org.springframework.test.util.ReflectionTestUtils.setField(service, "roleId", "master-role");
        var request = new MasterChangeNotifyReq();
        request.setRoleId(matchingRole ? "master-role" : "other-role");

        assertEquals(enabled && matchingRole, service.handleMasterChange(request).isSuccess());
        verify(election, times(enabled && matchingRole ? 1 : 0)).updateLatestMaster();
    }
}
