package com.pagoda.etl;

import com.pagoda.etl.canal.ext.CanalMessageDeserializationSchema;
import com.pagoda.etl.canal.ext.Message;
import com.pagoda.etl.utils.Order;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.queryablestate.network.messages.MessageDeserializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.List;
import java.util.Properties;

/**
 * 2
 *
 * @author xiexiong
 */
public class OmsOrderEtl {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //并行度设置为1

        Properties props = new Properties();
        props.put("auto.offset.reset", "latest");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", MessageDeserializer.class.getName());
        //props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("group.id", "test");
        props.put("bootstrap.servers", "10.8.29.3:9092,10.8.29.12:9092,10.8.29.15:9092");


        //DataStreamSource<List<String>> orderStream = env
        //       .addSource(new FlinkKafkaConsumer<>("ods_oms_orders", new CanalMessageDeserializationSchema(), props));


        DataStream<List<String>> payMentStream = env
                .addSource(new FlinkKafkaConsumer<>("ods_oms_payment_orders", new CanalMessageDeserializationSchema(), props)); //序列化成 List<String> 类型的数据

     /* DataStream orderStreamflat=  orderStream.flatMap(new FlatMapFunction<List<String>, Object>() {

            @Override
            public void flatMap(List<String> strings, Collector<Object> collector) throws Exception {
               // strings.forEach(s -> s.split("================>"));
            }
        });*/

        //orderStreamflat.print();
        payMentStream.print();
        env.execute("oms");
    }
}
