package net.envsensor.kafka.connect.smt;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.transforms.util.SimpleConfig;

import java.util.Map;

public final class TransformConfig {
    private TransformConfig() {}

    public static final String ON_ERROR = "on.error";
    public static final String ON_ERROR_DOC =
            "Error handling strategy when payload parsing fails. " +
            "Allowed: fail (throw), drop (return null), pass (leave record unchanged).";

    public enum OnErrorMode { FAIL, DROP, PASS }

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(
                    ON_ERROR,
                    ConfigDef.Type.STRING,
                    "fail",
                    ConfigDef.ValidString.in("fail", "drop", "pass"),
                    ConfigDef.Importance.MEDIUM,
                    ON_ERROR_DOC
            );

    public static OnErrorMode onError(Map<String, ?> props) {
        SimpleConfig cfg = new SimpleConfig(CONFIG_DEF, props);
        String v = cfg.getString(ON_ERROR).trim().toLowerCase();
        return switch (v) {
            case "drop" -> OnErrorMode.DROP;
            case "pass" -> OnErrorMode.PASS;
            default -> OnErrorMode.FAIL;
        };
    }
}
