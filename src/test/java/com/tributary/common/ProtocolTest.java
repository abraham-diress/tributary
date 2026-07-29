package com.tributary.common;

import com.tributary.storage.CorruptRecordException;
import com.tributary.storage.Record;
import com.tributary.storage.Records;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolTest {

    @Test
    void produceRequestRoundTrip() {
        ByteBuffer buf = Protocol.encodeProduceRequest("orders", 2, Protocol.ACKS_ALL, 5000,
                List.of(Record.of("k1", "v1"), Record.of(null, "v2")));
        assertEquals(Protocol.PRODUCE, buf.get());
        Protocol.ProduceRequest req = Protocol.decodeProduceRequest(buf);
        assertEquals("orders", req.topic());
        assertEquals(2, req.partition());
        assertEquals(Protocol.ACKS_ALL, req.acks());
        assertEquals(2, req.records().size());
        assertEquals("k1", req.records().get(0).keyAsString());
        assertEquals("v2", req.records().get(1).valueAsString());
    }

    @Test
    void fetchResponseCarriesRawRecords() throws Exception {
        ByteBuffer records = Records.encode(40, List.of(Record.of("a", "1"), Record.of("b", "2")), 1L);
        ByteBuffer buf = Protocol.encodeFetchResponse(Errors.NONE, null, 42, 0, 50, records);
        Protocol.FetchResult result = Protocol.decodeFetchResponse(buf);
        assertEquals(42, result.highWatermark());
        assertEquals(50, result.logEndOffset());
        assertEquals(41, result.records().get(1).offset());
    }

    @Test
    void outOfRangeFetchResponseBecomesTypedException() {
        ByteBuffer buf = Protocol.encodeFetchResponse(Errors.OFFSET_OUT_OF_RANGE, "too far", 5, 3, 9, ByteBuffer.allocate(0));
        OffsetOutOfRangeException e = assertThrows(OffsetOutOfRangeException.class, () -> Protocol.decodeFetchResponse(buf));
        assertEquals(3, e.logStartOffset());
        assertEquals(9, e.logEndOffset());
    }

    @Test
    void errorResponsesAreRaisedWithCode() {
        ByteBuffer buf = Protocol.errorResponse(Protocol.PRODUCE, Errors.NOT_LEADER, "leader is 2");
        KafkaException e = assertThrows(KafkaException.class, () -> Protocol.decodeProduceResponse(buf));
        assertEquals(Errors.NOT_LEADER, e.code());
        assertEquals("leader is 2", e.detail());
        assertEquals(true, e.isRetriable());
    }

    @Test
    void metadataRoundTrip() throws Exception {
        ByteBuffer buf = Protocol.encodeMetadataResponse(2,
                List.of(new BrokerInfo(1, "localhost", 9091), new BrokerInfo(2, "localhost", 9092)),
                List.of(new Protocol.TopicMetadata("t", List.of(new Protocol.PartitionMetadata(0, 2, 3, List.of(2, 1), List.of(2))))));
        Protocol.MetadataResult md = Protocol.decodeMetadataResponse(buf);
        assertEquals(2, md.controllerId());
        assertEquals(9092, md.broker(2).getPort());
        Protocol.PartitionMetadata p = md.topic("t").partition(0);
        assertEquals(2, p.leader());
        assertEquals(3, p.leaderEpoch());
        assertEquals(List.of(2, 1), p.replicas());
        assertEquals(List.of(2), p.isr());
    }

    @Test
    void corruptedRecordFailsCrcCheck() {
        ByteBuffer records = Records.encode(0, List.of(Record.of("key", "value")), 1L);
        records.put(records.limit() - 1, (byte) 'X');
        assertThrows(CorruptRecordException.class, () -> Records.decode(records));
    }
}
