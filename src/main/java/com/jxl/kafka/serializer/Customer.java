package com.jxl.kafka.serializer;


/**
 * Created by zhujiadong on 2018/4/24 14:54
 */

public class Customer {
    private int customerID;
    private String customerName;

    public int getCustomerID() {
        return customerID;
    }

    public String getCustomerName() {
        return customerName;
    }

    public Customer(int ID, String name){
        this.customerID = ID;
        this.customerName = name;

    }

}
