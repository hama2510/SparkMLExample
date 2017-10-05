/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample.domain;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author KienNT
 */
public class Transaction {

    private Integer user;
    private List<Integer> items;

    public Transaction() {
        items = new ArrayList();
    }

    public Transaction(Integer user) {
        items = new ArrayList();
        this.user = user;
    }

    public Integer getUser() {
        return user;
    }

    public List<Integer> getItems() {
        return items;
    }

    public void addItem(Integer item) {
        items.add(item);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Transaction) {
            if (obj != null) {
                if (((Transaction) obj).user != null && ((Transaction) obj).user.equals(user)) {
                    return true;
                }
            }
        }
        return false;
    }
}
