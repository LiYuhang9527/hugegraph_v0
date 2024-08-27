package com.genfile.HBase.HugeGraph.direct.util;

import org.apache.hugegraph.util.E;

public interface Id extends Comparable<Id> {

    public static final int UUID_LENGTH = 16;

    public Object asObject();

    public String asString();

    public long asLong();

    public byte[] asBytes();

    public int length();

    public IdType type();

    public default boolean number() {
        return this.type() == IdType.LONG;
    }

    public default boolean uuid() {
        return this.type() == IdType.UUID;
    }

    public default boolean string() {
        return this.type() == IdType.STRING;
    }

    public default boolean edge() {
        return this.type() == IdType.EDGE;
    }

    public enum IdType {

        UNKNOWN,
        LONG,
        UUID,
        STRING,
        EDGE;

        public char prefix() {
            if (this == UNKNOWN) {
                return 'N';
            }
            return this.name().charAt(0);
        }

        public static IdType valueOfPrefix(String id) {
            E.checkArgument(id != null && id.length() > 0,
                    "Invalid id '%s'", id);
            switch (id.charAt(0)) {
                case 'L':
                    return IdType.LONG;
                case 'U':
                    return IdType.UUID;
                case 'S':
                    return IdType.STRING;
                case 'E':
                    return IdType.EDGE;
                default:
                    return IdType.UNKNOWN;
            }
        }
    }
}
