package com.entities;

public class ItemCount {

    String id;

    int count;

    public ItemCount() {
    }

    public ItemCount(String id, int count) {
        this.id = id;
        this.count = count;
    }

    public ItemCount setCount(int count) {
        this.count = count;
        return this;
    }

    public int getCount() {
        return this.count;
    }

    public ItemCount setId(String id) {
        this.id = id;
        return this;
    }

    public String getId() {
        return this.id;
    }
}
