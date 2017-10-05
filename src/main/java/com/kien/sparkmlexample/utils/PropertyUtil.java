/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample.utils;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author KienNT
 */
public class PropertyUtil {

    public static Properties getProperty(String filePath) {
        Properties prop = new Properties();
        InputStream input = PropertyUtil.class.getClassLoader().getResourceAsStream(filePath);
        try {
            prop.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Cannot open property file!");
        }
        try {
            input.close();
        } catch (IOException ex) {
            Logger.getLogger(PropertyUtil.class.getName()).log(Level.SEVERE, null, ex);
        }
        return prop;
    }
}
