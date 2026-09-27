package com.github.lisama569.lakehousesnapshotservicejava.model;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 一行数据，JSON 形态就是列名->值 的扁平对象（{"id":1,"name":"a",...}）。
 * 通过 {@link JsonValue}/{@link JsonCreator} 让 record 与该扁平 JSON 直接互转。
 */
public record RowRecord(Map<String, Object> values) {

    @JsonCreator
    public static RowRecord fromMap(Map<String, Object> values) {
        return new RowRecord(values == null ? Map.of() : new LinkedHashMap<>(values));
    }

    @JsonValue
    @JsonAnyGetter
    public Map<String, Object> values() {
        return values;
    }
}
