package org.apache.flink;

import org.apache.flink.annotation.Public;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * 显而易见此类为flink版本枚举类， 多用在SQL/Table API升级，还有迁移测试的场景
 *
 * @author DeveloperZJQ
 * @since 2022-7-19
 */
@Public
public enum FlinkVersion {

    // NOTE: the version strings must not change,
    // as they are used to locate snapshot file paths.
    // The definition order (enum ordinal) matters for performing version arithmetic.
    v1_3("1.3"),
    v1_4("1.4"),
    v1_5("1.5"),
    v1_6("1.6"),
    v1_7("1.7"),
    v1_8("1.8"),
    v1_9("1.9"),
    v1_10("1.10"),
    v1_11("1.11"),
    v1_12("1.12"),
    v1_13("1.13"),
    v1_14("1.14"),
    v1_15("1.15"),
    v1_16("1.16");

    private final String versionStr;

    FlinkVersion(String versionStr) {
        this.versionStr = versionStr;
    }

    @Override
    public String toString() {
        return versionStr;
    }

    public boolean isNewerVersionThan(FlinkVersion otherVersion) {
        return this.ordinal() > otherVersion.ordinal();
    }

    /**
     * Returns all versions within the defined range, inclusive both start and end.
     */
    public static Set<FlinkVersion> rangeOf(FlinkVersion start, FlinkVersion end) {
        return Stream.of(FlinkVersion.values())
                .filter(v -> v.ordinal() >= start.ordinal() && v.ordinal() <= end.ordinal())
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }

    private static final Map<String, FlinkVersion> CODE_MAP =
            Arrays.stream(values())
                    .collect(Collectors.toMap(v -> v.versionStr, Function.identity()));

    public static Optional<FlinkVersion> byCode(String code) {
        return Optional.ofNullable(CODE_MAP.get(code));
    }

    /**
     * Returns the current version.
     */
    public static FlinkVersion current() {
        return values()[values().length - 1];
    }
}