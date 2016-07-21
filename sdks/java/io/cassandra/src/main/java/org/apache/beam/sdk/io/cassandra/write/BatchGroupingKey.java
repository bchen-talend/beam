package org.apache.beam.sdk.io.cassandra.write;

/**
 *
 */
public enum BatchGroupingKey {
    None, Replica, Partition;

    public static BatchGroupingKey fromString(String name) {
        if (name != null) {
            for (BatchGroupingKey b : BatchGroupingKey.values()) {
                if (name.equalsIgnoreCase(b.name())) {
                    return b;
                }
            }
        }
        return Partition;
    }
}
