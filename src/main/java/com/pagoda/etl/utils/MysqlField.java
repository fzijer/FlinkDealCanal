package com.pagoda.etl.utils;

import lombok.Builder;
import lombok.Data;

/**
 * @Package com.pagoda.model
 * @Author xiexiong
 * @Date 22:43 2019-12-13
 * @Description 表示一个mysql字段包含 字段名 字段类型 字段值
 */
@Data
@Builder
public class MysqlField {
    private String fieldName;
    private String fieldType;
    private String fieldValue;
}
