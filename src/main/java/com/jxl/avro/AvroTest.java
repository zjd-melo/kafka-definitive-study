package com.jxl.avro;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by zhujiadong on 2018/4/24 16:03
 */

/**
 * schema伴随着数据文件的每一条记录
 */
public class AvroTest {
    public static void main(String[] args) throws IOException {
        AvroTest avroTest = new AvroTest();
//        avroTest.serilizer();
        avroTest.read();

    }
    private void serilizer() throws IOException {
        User user1 = new User();
        user1.setName("melo");
        user1.setAge(22);
        user1.setSex("male");

        User user2 = new User("marry", 22, "female");

        User user3 = User.newBuilder().setName("Rilley").setAge(35).setSex("female").build();

        //三种方式 创建对象 使用构造器创建的效率最高 使用build创建的对象会把schema定义的默认值赋值给属性,并且校验数据是否符合schema
        //但是构造器不会这么做知道对象被序列化才校验

        DatumWriter<User> writer = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(writer);
        dataFileWriter.create(user1.getSchema(), new File("user.avro"));
        dataFileWriter.append(user1);
        dataFileWriter.append(user2);
        dataFileWriter.append(user3);
        dataFileWriter.close();
    }

    private void read() throws IOException {
        DatumReader<User> datumReader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = new DataFileReader<User>(new File("user.avro"), datumReader);
        while (dataFileReader.hasNext()){
            System.out.println(dataFileReader.next());
        }
    }
}
