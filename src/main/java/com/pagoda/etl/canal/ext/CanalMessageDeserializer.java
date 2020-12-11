package com.pagoda.etl.canal.ext;

import com.google.protobuf.ByteString;

public class CanalMessageDeserializer {

    public static Message deserializer(byte[] data) {
        return deserializer(data, false);
    }

    public static Message deserializer(byte[] data, boolean lazyParseEntry) {
        try {
            if (data == null) {
                return null;
            } else {
                CanalPacket.Packet p = CanalPacket.Packet.parseFrom(data);
                switch (p.getType()) {
                    case MESSAGES: {
                        if (!p.getCompression().equals(CanalPacket.Compression.NONE)
                            && !p.getCompression().equals(CanalPacket.Compression.COMPRESSIONCOMPATIBLEPROTO2)) {
                            throw new CanalClientException("compression is not supported in this connector");
                        }

                        CanalPacket.Messages messages = CanalPacket.Messages.parseFrom(p.getBody());        //Message 多条sql 引起的数据变化
                        Message result = new Message(messages.getBatchId());        //给Message对象中的 id赋值
                        if (lazyParseEntry) {
                            // byteString
                            result.setRawEntries(messages.getMessagesList());
                            result.setRaw(true);
                        } else { //Iterable<Byte>
                            for (ByteString byteString : messages.getMessagesList()) {          //开始遍历 一个 Message
                                result.addEntry(CanalEntry.Entry.parseFrom(byteString));        // 把 遍历的 Message  遍历成 多个Entry  (一个ENtry 代表一个 sql)
                            }
                            result.setRaw(false);                                               //把Raw 设置 为 false
                        }
                        return result;                                                          //返回Message对象
                    }
                    case ACK: {
                        CanalPacket.Ack ack = CanalPacket.Ack.parseFrom(p.getBody());
                        throw new CanalClientException("something goes wrong with reason: " + ack.getErrorMessage());
                    }
                    default: {
                        throw new CanalClientException("unexpected packet type: " + p.getType());
                    }
                }
            }
        } catch (Exception e) {
            throw new CanalClientException("deserializer failed", e);
        }
    }
}
