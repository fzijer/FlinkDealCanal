package com.pagoda.etl.canal.ext;

import cn.hutool.json.JSONObject;
import com.pagoda.etl.utils.EventToJsonUtils;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;

//序列化成 List<String> 类型的数据
public class CanalMessageDeserializationSchema implements KafkaDeserializationSchema<List<String>> {

    @Override
    public boolean isEndOfStream(List<String> nextElement) {
        return false;
    }

    @Override
    public List<String> deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
        //将 kafka中的数据一个record  封装在 一个Message对象中， 一个message对象中 有多个Entry   message.getEntries() 返回 List<CanalEntry.Entry> 传入toJsonList 中
        Message message = CanalMessageDeserializer.deserializer(record.value());
        return EventToJsonUtils.toJsonList(message.getEntries());   //将 多条 Entry 放在List 中 并转成Json格式
    }

    @Override
    public TypeInformation<List<String>> getProducedType() {
        return TypeInformation.of(new TypeHint<List<String>>() {});
    }
}
