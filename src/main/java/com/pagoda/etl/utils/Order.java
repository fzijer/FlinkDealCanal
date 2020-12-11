package com.pagoda.etl.utils;

public class Order {
    private  String address;
    private  String order_sn;
    private  String channel_order_id;
    private  String status;

    public Order(String address, String order_sn, String channel_order_id, String status) {
        this.address = address;
        this.order_sn = order_sn;
        this.channel_order_id = channel_order_id;
        this.status = status;
    }

    public String getAddress() {
        return address;
    }

    public String getOrder_sn() {
        return order_sn;
    }

    public String getChannel_order_id() {
        return channel_order_id;
    }

    public String getStatus() {
        return status;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public void setOrder_sn(String order_sn) {
        this.order_sn = order_sn;
    }

    public void setChannel_order_id(String channel_order_id) {
        this.channel_order_id = channel_order_id;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return "Order{" +
                "address='" + address + '\'' +
                ", order_sn='" + order_sn + '\'' +
                ", channel_order_id='" + channel_order_id + '\'' +
                ", status='" + status + '\'' +
                '}';
    }
}
