package com.tributary.broker;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtocolTest {

    @Test
    void encodesProduceRequest() {
        ByteBuffer buffer = Protocol.encodeProduceRequest("orders", 2, "hello".getBytes(StandardCharsets.UTF_8));
        assertEquals(Protocol.PRODUCE, buffer.get());
        assertEquals("orders", Protocol.getString(buffer));
        assertEquals(2, buffer.getInt());
        byte[] message = new byte[buffer.getInt()];
        buffer.get(message);
        assertEquals("hello", new String(message, StandardCharsets.UTF_8));
    }

    @Test
    void decodesProduceResponse() {
        Protocol.ProduceResult result = Protocol.decodeProduceResponse(Protocol.encodeProduceResponse(42));
        assertEquals(42, result.getOffset());
        assertTrue(!result.hasError());
    }

    @Test
    void decodesFetchResponse() {
        List<byte[]> messages = List.of("a".getBytes(), "bb".getBytes());
        Protocol.FetchResult result = Protocol.decodeFetchResponse(Protocol.encodeFetchResponse(10, messages));
        assertEquals(2, result.getMessages().size());
        assertEquals("bb", new String(result.getMessages().get(1)));
    }

    @Test
    void decodesMetadataResponse() {
        ByteBuffer buffer = Protocol.encodeMetadataResponse(
                List.of(new BrokerInfo(1, "localhost", 9091), new BrokerInfo(2, "localhost", 9092)),
                List.of(new Protocol.TopicMetadata("orders",
                        List.of(new Protocol.PartitionMetadata(0, 2, List.of(2, 1))))));
        Protocol.MetadataResult result = Protocol.decodeMetadataResponse(buffer);
        assertEquals(2, result.getBrokers().size());
        assertEquals(9092, result.getBrokers().get(1).getPort());
        Protocol.PartitionMetadata partition = result.getTopics().get(0).getPartitions().get(0);
        assertEquals(2, partition.getLeader());
        assertEquals(List.of(2, 1), partition.getReplicas());
    }

    @Test
    void errorsAreReported() {
        Protocol.ProduceResult result = Protocol.decodeProduceResponse(Protocol.encodeErrorResponse("Unknown topic"));
        assertTrue(result.hasError());
        assertEquals("Unknown topic", result.getError());
    }
}

