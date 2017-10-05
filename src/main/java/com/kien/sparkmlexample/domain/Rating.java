/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample.domain;

/**
 *
 * @author KienNT
 */
public class Rating {
    private Integer user;
    private Integer item;
    private Float rating;

    public Rating() {
    }

    public Rating(Integer user, Integer item, Float rating) {
        this.user = user;
        this.item = item;
        this.rating = rating;
    }

    public Integer getUser() {
        return user;
    }

    public Integer getItem() {
        return item;
    }

    public Float getRating() {
        return rating;
    }
    
    public static Rating parseRating100k(String str) {
        String[] fields = str.split("\t");
        if (fields.length != 4) {
            throw new IllegalArgumentException("Each line must contain 4 fields");
        }
        int userId = Integer.parseInt(fields[0]);
        int movieId = Integer.parseInt(fields[1]);
        float rating = Float.parseFloat(fields[2]);
        return new Rating(userId, movieId, rating);
    }
}
