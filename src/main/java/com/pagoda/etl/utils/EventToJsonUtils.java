package com.pagoda.etl.utils;


import cn.hutool.json.JSONUtil;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.pagoda.etl.canal.ext.CanalEntry;

import java.util.List;
import java.util.Map;

/**
 * @author xiexiong
 * @date
 */
public class EventToJsonUtils {
    public static List<String> toJsonList(List<CanalEntry.Entry> entries) {
        List<String> resultList =Lists.newArrayList();
        for (CanalEntry.Entry entry : entries) {        //遍历多个 entry one sql
            if (entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONBEGIN || entry.getEntryType() == CanalEntry.EntryType.TRANSACTIONEND) {
                continue;
            }

            CanalEntry.RowChange rowChange = null;      //RowChange 多行的变化 对象
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());  //一个entry 得到一个getStoreValue ，得到一个RowChange
            } catch (Exception e) {
                throw new RuntimeException("ERROR ## parser of eromanga-event has an error , data:" + entry.toString(),
                        e);
            }

            //RowChange   得到 该sql 的类型
            CanalEntry.EventType eventType = rowChange.getEventType();
            String tableName = entry.getHeader().getTableName();
            String schemaName = entry.getHeader().getSchemaName();
            long executeTime = entry.getHeader().getExecuteTime();

            //根据binlog的filename和position来定位 //
            System.out.println(String.format("================> binlog[%s:%s] , name[%s,%s] , eventType : %s",
                    entry.getHeader().getLogfileName(), entry.getHeader().getLogfileOffset(),
                    entry.getHeader().getSchemaName(), entry.getHeader().getTableName(),
                    eventType));
            resultList.addAll(rowDataToJSONListSimple(rowChange, eventType, tableName, schemaName, executeTime));       //调用 rowDataToJSONListSimple 方法

        }
        return resultList;
    }

    //遍历 rowchange 里的多个 rowdata
    private static List<String> rowDataToJSONListSimple(CanalEntry.RowChange rowChange, CanalEntry.EventType eventType, String tableName, String schemaName, long executeTime) {
        List<String> jsonList = Lists.newArrayListWithCapacity(rowChange.getRowDatasList().size()); //得到 该 rowchage  有多少个 rowdata   一个rowdata 代表一行数据的封装
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String, Object> map = getSimpleStringObjectMap(eventType, tableName, schemaName, executeTime, rowData);     //getSimpleStringObjectMap 处理一个 rowdata
            jsonList.add(JSONUtil.toJsonStr(map));      // toJsonStr(map) 得到一个 Json 字符串
            System.out.println(JSONUtil.toJsonStr(map));
        }
        return jsonList;
    }

    private static List<Object> rowDataToJsonList(CanalEntry.RowChange rowChange, CanalEntry.EventType eventType, String tableName, String schemaName, long executeTime) {
        List<Object> jsonList = Lists.newArrayListWithCapacity(rowChange.getRowDatasList().size());
        for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
            Map<String, Object> map = getStringObjectMap(eventType, tableName, schemaName, executeTime, rowData);
            jsonList.add(JSONUtil.toJsonStr(map));
        }
        return jsonList;
    }

    //getSimpleStringObjectMap  处理一个 rowdata  将数据解析成 最终的数据
    private static Map<String, Object> getSimpleStringObjectMap(CanalEntry.EventType eventType, String tableName, String schemaName, long executeTime, CanalEntry.RowData rowData) {
        Map<String, Object> map = Maps.newLinkedHashMap();
        map.put("userId", null);
        map.put("userName", null);
        map.put("password", null);
        map.put("phone", null);
        map.put("update_time", null);
        map.put("not_real", null);
        map.put("operation_type", null);
        List<CanalEntry.Column> data = null;            //对不同的sql类型 给map中的 operation_type  的value 赋值
        if (eventType == CanalEntry.EventType.DELETE) {
            map.put("operation_type", Operation.DELETE.getValue());
            data = rowData.getBeforeColumnsList();
        } else if (eventType == CanalEntry.EventType.INSERT) {
            map.put("operation_type", Operation.INSERT.getValue());
            data = rowData.getAfterColumnsList();
        } else {
            map.put("operation_type", Operation.UPDATE.getValue());
            data = rowData.getAfterColumnsList();
        }
        for (CanalEntry.Column column : data) {
            if (!Strings.isNullOrEmpty(column.getValue())) {
                map.put(column.getName(), column.getValue());
            } else {
                map.put(column.getName(), column.getValue());
            }
        }
        return map;
    }

    ;

    public static Map<String, Object> getStringObjectMap(CanalEntry.EventType eventType, String tableName, String schemaName, long executeTime, CanalEntry.RowData rowData) {
        Map<String, Object> map = Maps.newTreeMap();
        map.put(EtlConstant.EVENT_TIMESTAMP, executeTime);
        map.put(EtlConstant.TABLE_NAME, tableName);
        map.put(EtlConstant.DATABASE_NAME, schemaName);
        if (eventType == CanalEntry.EventType.DELETE) {
            map.put(Operation.DELETE.getKey(), Operation.DELETE.getValue());
            map.put(EtlConstant.COLUMN, getDataMap(rowData.getBeforeColumnsList()));
            List<String> keys = getDataKey(rowData.getBeforeColumnsList());
            map.put(EtlConstant.KEYS, keys);
        } else if (eventType == CanalEntry.EventType.INSERT) {
            map.put(Operation.INSERT.getKey(), Operation.INSERT.getValue());
            map.put(EtlConstant.COLUMN, getDataMap(rowData.getAfterColumnsList()));
            List<String> keys = getDataKey(rowData.getAfterColumnsList());
            map.put(EtlConstant.KEYS, keys);
        } else {
            map.put(Operation.UPDATE.getKey(), Operation.UPDATE.getValue());
            map.put(EtlConstant.COLUMN, getDataMap(rowData.getAfterColumnsList()));
            List<String> keys = getDataKey(rowData.getAfterColumnsList());
            map.put(EtlConstant.KEYS, keys);

            Map<String, Object> beforeMap = getDataMap(rowData.getBeforeColumnsList());
            map.put(EtlConstant.BEFORE_COLUMN, beforeMap);
        }
        //            List<Object> tempList= map.entrySet().stream().map(entry-> {
//                if(entry.getKey().equals("userId")||entry.getKey().equals("event_timestamp")){
//                    return entry.getValue();
//                }else {
//                   return "\"" + entry.getValue() + "\"";
//                }
//            }).collect(Collectors.toList());
        System.out.println(JSONUtil.toJsonStr(map));
//            jsonList.add(StringUtils.join(tempList.toArray(),","));
        return map;
    }

    private static List<String> getDataKey(List<CanalEntry.Column> data) {
        List<String> keys = Lists.newArrayList();
        for (CanalEntry.Column column : data) {
            if (column.getIsKey()) {
                keys.add(column.getName());
            }
        }
        return keys;
    }


    private static Map<String, Object> getDataMap(List<CanalEntry.Column> data) {
        Map<String, Object> dataMap = Maps.newHashMapWithExpectedSize(data.size());
        for (CanalEntry.Column column : data) {
            if (!Strings.isNullOrEmpty(column.getValue())) {
                MysqlField field = MysqlField.builder()
                        .fieldName(column.getName())
                        .fieldType(column.getMysqlType())
                        .fieldValue(column.getValue())
                        .build();
                dataMap.put(column.getName(), field);
            }
        }
        return dataMap;
    }
}
