package net.envsensor.kafka.connect.smt;

import envsensor.vernemq.EnrichedReading;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.components.Versioned;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

/**
 * SMT that decodes envsensor.vernemq.EnrichedReading protobuf from raw BYTES
 * and outputs a single CSV row as UTF-8 BYTES (no trailing newline).
 *
 * Intended for Aiven S3 sink with:
 *   format.output.type=csv
 *   format.output.fields=value
 *   format.output.fields.value.encoding=none
 *
 * CSV output is ClickHouse-friendly:
 * - ipv4 is dotted-decimal string (e.g. 192.168.1.10) or empty
 * - ipv6 is standard textual IPv6 string (e.g. 2001:db8::1) or empty
 * - mac is 12-char lower hex (48-bit) string (e.g. 112233445566)
 */
public abstract class EnvsensorProtobufToCsv<R extends ConnectRecord<R>>
        implements Transformation<R>, Versioned {

    // ---------------- Config ----------------
    public static final String ON_ERROR = "on.error"; // fail|drop|pass
    public static final String CSV_DELIMITER = "csv.delimiter"; // single char
    public static final String CSV_SANITIZE_NEWLINES = "csv.sanitize.newlines"; // true/false
    public static final String REQUIRE_BYTES = "require.bytes"; // true/false

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ON_ERROR, ConfigDef.Type.STRING, "fail",
                    ConfigDef.ValidString.in("fail", "drop", "pass"),
                    ConfigDef.Importance.MEDIUM, "Error handling: fail|drop|pass")
            .define(CSV_DELIMITER, ConfigDef.Type.STRING, ",",
                    ConfigDef.Importance.LOW, "CSV delimiter (single character), default ','")
            .define(CSV_SANITIZE_NEWLINES, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.LOW, "Replace CR/LF in string fields with literal \\n")
            .define(REQUIRE_BYTES, ConfigDef.Type.BOOLEAN, true,
                    ConfigDef.Importance.MEDIUM, "If true, throw when record value is not BYTES");

    private TransformConfig.OnErrorMode onErrorMode = TransformConfig.OnErrorMode.FAIL;
    private char delimiter = ',';
    private boolean sanitizeNewlines = true;
    private boolean requireBytes = true;

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.onErrorMode = TransformConfig.onError(configs);

        // delimiter
        String ds = getString(configs, CSV_DELIMITER, ",");
        if (ds.length() != 1) {
            throw new DataException("csv.delimiter must be a single character, got: " + ds);
        }
        this.delimiter = ds.charAt(0);
        if (this.delimiter == '\n' || this.delimiter == '\r') {
            throw new DataException("csv.delimiter cannot be newline");
        }

        // booleans may arrive as String
        this.sanitizeNewlines = getBoolean(configs, CSV_SANITIZE_NEWLINES, true);
        this.requireBytes = getBoolean(configs, REQUIRE_BYTES, true);
    }

    @Override
    public R apply(R record) {
        try {
            final Object raw = operatingValue(record);
            final byte[] bytes = toByteArray(raw);

            if (bytes == null) {
                if (requireBytes) {
                    throw new DataException("Record value must be BYTES (byte[]/ByteBuffer) for EnvsensorProtobufToCsv");
                }
                return record; // pass-through if configured
            }

            final EnrichedReading msg = EnrichedReading.parseFrom(bytes);

            // Build ONE CSV row (no trailing newline).
            final String row = buildCsvRow(record, msg, delimiter, sanitizeNewlines);
            final byte[] outBytes = row.getBytes(StandardCharsets.UTF_8);

            return newRecord(record, Schema.BYTES_SCHEMA, outBytes);

        } catch (Exception e) {
            return handleError(record, e);
        }
    }

    private R handleError(R record, Exception e) {
        switch (onErrorMode) {
            case DROP:
                return null;
            case PASS:
                return record;
            case FAIL:
            default:
                if (e instanceof RuntimeException) throw (RuntimeException) e;
                throw new DataException("EnvsensorProtobufToCsv failed", e);
        }
    }

    @Override
    public void close() {
        // no-op
    }

    @Override
    public String version() {
        return "0.2.0";
    }

    protected abstract Object operatingValue(R record);
    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    // ---------------- CSV formatting ----------------

    private static String buildCsvRow(
            ConnectRecord<?> record,
            EnrichedReading msg,
            char delimiter,
            boolean sanitizeNewlines
    ) {
        StringBuilder sb = new StringBuilder(256);

        // Column order (stable, no header here):
        // time_ns, topic, ipv4, ipv6, user, clientid, broker, mac, rssi, batt, esp32_t,
        // bme280_t, bme280_p, bme280_h, sht4x_t, sht4x_h,
        // kafka_topic, kafka_partition, kafka_offset, kafka_timestamp_ms, kafka_key, ingested_at_ms

        appendLong(sb, (long) msg.getTime());
        appendDelim(sb, delimiter);

        appendString(sb, msg.getTopic(), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendString(sb, ipv4ToDotted(msg.getIpv4()), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendString(sb, ipv6ToText(msg.getIpv6()), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendString(sb, msg.getUser(), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendString(sb, msg.getClientid(), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendString(sb, msg.getBroker(), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        // mac as 12-char lower hex (48-bit masked)
        appendString(sb, macToHex48(msg.getMac()), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendInt(sb, msg.getRssi());
        appendDelim(sb, delimiter);

        appendInt(sb, (int) msg.getBatt());
        appendDelim(sb, delimiter);

        appendFloat(sb, msg.getEsp32T());
        appendDelim(sb, delimiter);

        appendNullableFloat(sb, msg.hasBme280T() ? msg.getBme280T() : null);
        appendDelim(sb, delimiter);

        appendNullableFloat(sb, msg.hasBme280P() ? msg.getBme280P() : null);
        appendDelim(sb, delimiter);

        appendNullableFloat(sb, msg.hasBme280H() ? msg.getBme280H() : null);
        appendDelim(sb, delimiter);

        appendNullableFloat(sb, msg.hasSht4XT() ? msg.getSht4XT() : null);
        appendDelim(sb, delimiter);

        appendNullableFloat(sb, msg.hasSht4XH() ? msg.getSht4XH() : null);
        appendDelim(sb, delimiter);

        // Kafka metadata (from record)
        appendString(sb, record.topic(), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        appendNullableInt(sb, record.kafkaPartition());
        appendDelim(sb, delimiter);

        Long kafkaOffset = (record instanceof SinkRecord) ? ((SinkRecord) record).kafkaOffset() : null;
        appendNullableLong(sb, kafkaOffset);
        appendDelim(sb, delimiter);

        // record.timestamp() is in milliseconds since epoch (Kafka record timestamp)
        appendNullableLong(sb, record.timestamp());
        appendDelim(sb, delimiter);

        appendString(sb, toKeyString(record.key()), delimiter, sanitizeNewlines);
        appendDelim(sb, delimiter);

        // ingested_at_ms: wall clock at SMT apply time
        appendLong(sb, System.currentTimeMillis());

        return sb.toString();
    }

    private static void appendDelim(StringBuilder sb, char delimiter) {
        sb.append(delimiter);
    }

    private static void appendString(StringBuilder sb, String v, char delimiter, boolean sanitizeNewlines) {
        if (v == null) return;

        String s = v;
        if (sanitizeNewlines) {
            s = s.replace("\r\n", "\\n").replace("\n", "\\n").replace("\r", "\\n");
        }

        boolean needsQuote = false;
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == delimiter || c == '"' || c == '\n' || c == '\r') {
                needsQuote = true;
                break;
            }
        }

        if (!needsQuote) {
            sb.append(s);
            return;
        }

        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '"') sb.append("\"\"");
            else sb.append(c);
        }
        sb.append('"');
    }

    private static void appendInt(StringBuilder sb, int v) {
        sb.append(v);
    }

    private static void appendLong(StringBuilder sb, long v) {
        sb.append(v);
    }

    private static void appendFloat(StringBuilder sb, float v) {
        sb.append(v);
    }

    private static void appendNullableInt(StringBuilder sb, Integer v) {
        if (v == null) return;
        sb.append(v);
    }

    private static void appendNullableLong(StringBuilder sb, Long v) {
        if (v == null) return;
        sb.append(v);
    }

    private static void appendNullableFloat(StringBuilder sb, Float v) {
        if (v == null) return;
        sb.append(v);
    }

    private static byte[] toByteArray(Object raw) {
        if (raw == null) return null;
        if (raw instanceof byte[]) return (byte[]) raw;
        if (raw instanceof ByteBuffer) {
            ByteBuffer b = ((ByteBuffer) raw).slice();
            byte[] out = new byte[b.remaining()];
            b.get(out);
            return out;
        }
        return null;
    }

    private static String ipv4ToDotted(com.google.protobuf.ByteString ipv4) {
        if (ipv4 == null || ipv4.size() != 4) return "";
        byte[] b = ipv4.toByteArray();
        return (b[0] & 0xff) + "." + (b[1] & 0xff) + "." + (b[2] & 0xff) + "." + (b[3] & 0xff);
    }

    private static String ipv6ToText(com.google.protobuf.ByteString ipv6) {
        if (ipv6 == null || ipv6.size() != 16) return "";
        byte[] b = ipv6.toByteArray();
        try {
            // Produces a valid textual form; may be compressed depending on JVM implementation.
            return InetAddress.getByAddress(b).getHostAddress();
        } catch (UnknownHostException e) {
            return "";
        }
    }

    private static String macToHex48(long mac) {
        // mac is fixed64 in protobuf; only lower 48 bits are MAC address
        return String.format("%012x", mac & 0x0000FFFFFFFFFFFFL);
    }

    private static String toKeyString(Object key) {
        if (key == null) return "";
        if (key instanceof String) return (String) key;
        if (key instanceof byte[]) return "bytes(" + ((byte[]) key).length + ")";
        if (key instanceof ByteBuffer) return "bytes(" + ((ByteBuffer) key).remaining() + ")";
        return key.toString();
    }

    private static String getString(Map<String, ?> configs, String key, String def) {
        Object v = configs.get(key);
        if (v == null) return def;
        return v.toString();
    }

    private static boolean getBoolean(Map<String, ?> configs, String key, boolean def) {
        Object v = configs.get(key);
        if (v == null) return def;
        if (v instanceof Boolean) return (Boolean) v;
        String s = v.toString().trim().toLowerCase(Locale.ROOT);
        if ("true".equals(s)) return true;
        if ("false".equals(s)) return false;
        return def;
    }

    // Apply SMT to record VALUE
    public static class Value<R extends ConnectRecord<R>> extends EnvsensorProtobufToCsv<R> {
        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(
                    record.topic(),
                    record.kafkaPartition(),
                    record.keySchema(),
                    record.key(),
                    updatedSchema,
                    updatedValue,
                    record.timestamp()
            );
        }
    }
}
