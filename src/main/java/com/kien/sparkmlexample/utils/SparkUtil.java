/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample.utils;

import java.util.Properties;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author KienNT
 */
public class SparkUtil {

    private final SparkConf config;

    private SparkUtil(String appName, String master) {
        config = new SparkConf().setAppName(appName).setMaster(master);
    }

    public static SparkUtil build(String propertyFilePath) {
        Properties prop = PropertyUtil.getProperty(propertyFilePath);
        String hadoopDir = prop.getProperty("haddop.dir");
        String appName = prop.getProperty("spark.app.name");
        String master = prop.getProperty("spark.master");
        if (StringUtil.isBlank(master) || StringUtil.isBlank(appName) || StringUtil.isBlank(hadoopDir)) {
            throw new IllegalArgumentException("Wrong or missing properties!");
        } else {
            System.setProperty("hadoop.home.dir", hadoopDir);
            return new SparkUtil(appName, master);
        }
    }

    public SparkSession getSession() {
        return SparkSession.builder().config(config).getOrCreate();
    }

    public JavaSparkContext getContext() {
        return new JavaSparkContext(config);
    }
}
