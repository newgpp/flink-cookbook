package com.felix.entity;

public class GoodsOrder {

    private String type;

    private Long price;

    public GoodsOrder() {
    }

    public GoodsOrder(String type, Long price) {
        this.type = type;
        this.price = price;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }
}
