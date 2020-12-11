package com.pagoda.etl.utils;

import lombok.Getter;

/**
 * @author xiexiong
 * @date 2019-11-13
 */
public enum Operation {
    INSERT("event_op_type","insert"),
    DELETE("event_op_type","delete"),
    UPDATE("event_op_type","update")
    ;
    @Getter
    private String key;

    @Getter
    String value;

    private Operation(String key, String value) {
        this.key = key;
        this.value = value;
    }


}
