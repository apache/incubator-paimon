package org.apache.paimon.flink.widetable.utils;

public enum ColType {
    INT("int"),
    STRING("varchar(1000)");

    private String value;

    ColType(String value) {
        this.value = value;
    }
}
