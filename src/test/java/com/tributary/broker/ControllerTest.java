package com.tributary.broker;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class ControllerTest {

    @Test
    void healthyPartitionIsUnchanged() {
        PartitionState s = new PartitionState(1, 0, List.of(1, 2, 3), List.of(1, 2, 3));
        assertSame(s, Controller.nextState(s, Set.of(1, 2, 3)));
    }

    @Test
    void deadFollowerIsRemovedFromIsr() {
        PartitionState s = new PartitionState(1, 4, List.of(1, 2, 3), List.of(1, 2, 3));
        PartitionState next = Controller.nextState(s, Set.of(1, 3));
        assertEquals(1, next.leader());
        assertEquals(4, next.leaderEpoch(), "ISR change alone doesn't bump the epoch");
        assertEquals(List.of(1, 3), next.isr());
    }

    @Test
    void deadLeaderIsReplacedByFirstLiveIsrMemberAndEpochBumps() {
        PartitionState s = new PartitionState(1, 4, List.of(1, 2, 3), List.of(1, 3));
        PartitionState next = Controller.nextState(s, Set.of(2, 3));
        assertEquals(3, next.leader(), "broker 2 is alive but out of sync, so it must not be elected");
        assertEquals(5, next.leaderEpoch());
        assertEquals(List.of(3), next.isr());
    }

    @Test
    void noLiveIsrMemberTakesPartitionOfflineWithoutUncleanElection() {
        PartitionState s = new PartitionState(1, 2, List.of(1, 2), List.of(1));
        PartitionState offline = Controller.nextState(s, Set.of(2));
        assertEquals(-1, offline.leader());
        assertEquals(3, offline.leaderEpoch());
        assertEquals(List.of(1), offline.isr(), "ISR is preserved so the last in-sync replica can come back");

        PartitionState recovered = Controller.nextState(offline, Set.of(1, 2));
        assertEquals(1, recovered.leader());
        assertEquals(4, recovered.leaderEpoch());
    }

    @Test
    void replicasAreSpreadRoundRobin() {
        List<Integer> brokers = List.of(1, 2, 3);
        assertEquals(List.of(1, 2), Controller.assignReplicas(brokers, 0, 0, 2));
        assertEquals(List.of(2, 3), Controller.assignReplicas(brokers, 0, 1, 2));
        assertEquals(List.of(3, 1), Controller.assignReplicas(brokers, 0, 2, 2));
    }

    @Test
    void partitionStateJsonRoundTrip() {
        PartitionState s = new PartitionState(-1, 7, List.of(3, 1, 2), List.of());
        assertEquals(s, PartitionState.fromJson(s.toJson()));
    }
}
