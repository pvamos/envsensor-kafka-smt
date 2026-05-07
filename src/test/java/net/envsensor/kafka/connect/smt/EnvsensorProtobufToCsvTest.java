package net.envsensor.kafka.connect.smt;

import envsensor.vernemq.EnrichedReading;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class EnvsensorProtobufToCsvTest {

    @Test
    void decodesToUtf8BytesCsvRow() {
        EnrichedReading msg = EnrichedReading.newBuilder()
                .setTime(123456789L)
                .setTopic("envsensor/test")
                .setIpv4(com.google.protobuf.ByteString.copyFrom(new byte[]{(byte)192, (byte)168, 1, 10}))
                .setIpv6(com.google.protobuf.ByteString.copyFrom(new byte[16]))
                .setUser("u")
                .setClientid("c")
                .setBroker("b")
                .setMac(0x112233445566L)
                .setRssi(-42)
                .setBatt(3300)
                .setEsp32T(21.5f)
                .build();

        byte[] raw = msg.toByteArray();

        SourceRecord r = new SourceRecord(
                Collections.emptyMap(),
                Collections.emptyMap(),
                "sensors",
                0,
                null,
                null,
                null,
                raw,
                1700000000000L
        );

        EnvsensorProtobufToCsv.Value<SourceRecord> t = new EnvsensorProtobufToCsv.Value<>();
        t.configure(Collections.emptyMap());

        SourceRecord out = t.apply(r);

        assertNotNull(out.valueSchema());
        assertEquals(org.apache.kafka.connect.data.Schema.Type.BYTES, out.valueSchema().type());
        assertTrue(out.value() instanceof byte[]);

        String line = new String((byte[]) out.value(), StandardCharsets.UTF_8);

        // no newline in the SMT output
        assertFalse(line.contains("\n"));
        assertFalse(line.contains("\r"));

        // sanity: first columns include dotted ipv4
        assertTrue(line.startsWith("123456789,envsensor/test,192.168.1.10,"));

        // mac is hex
        assertTrue(line.contains(",112233445566,"));
    }
}
