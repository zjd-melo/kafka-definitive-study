package com.jxl.kafka.serializer;

/**
 * Created by zhujiadong on 2018/4/24 14:57
 */

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.nio.ByteBuffer;
import java.util.Map;

/**
 * 自定义序列化方式 建议使用第三方的序列化库(Avro, Thrift, Protobuf)
 */
public class CustomerSerializer implements Serializer<Customer> {
    public void configure(Map<String, ?> map, boolean b) {
        //nothing to configure
    }

    /**
     * 把customer序列化为:
     * 4 byte int 表示customerId
     * 4 byte int 表示customName在UTF-8编码的长度(0 if name is null)
     * N byte 表示UTF-8下的name
     *
     * @param s        topic
     * @param customer 要序列化的对象
     * @return byte array
     */
    public byte[] serialize(String s, Customer customer) {
        try {
            byte[] serializedName;
            int stringSize;
            if (customer == null) {
                return null;
            } else {
                if (customer.getCustomerName() != null) {
                    serializedName = customer.getCustomerName().getBytes("UTF-8");
                    stringSize = serializedName.length;
                } else {
                    serializedName = new byte[0];
                    stringSize = 0;
                }
            }
            ByteBuffer byteBuffer = ByteBuffer.allocate(4 + 4 + stringSize);
            byteBuffer.putInt(customer.getCustomerID());
            byteBuffer.putInt(stringSize);
            byteBuffer.put(serializedName);
            return byteBuffer.array();
        } catch (Exception e) {
            throw new SerializationException("Error when serializing Customer to byte[]" + e);
        }
    }

    public void close() {
        //nothing to close
    }
}

