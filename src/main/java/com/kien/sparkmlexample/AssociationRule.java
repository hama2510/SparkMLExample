/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample;

import com.kien.sparkmlexample.domain.Rating;
import com.kien.sparkmlexample.domain.Transaction;
import com.kien.sparkmlexample.utils.SparkUtil;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.fpm.FPGrowth;
import org.apache.spark.ml.fpm.FPGrowthModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 *
 * @author KienNT
 */
public class AssociationRule {

    public static void main(String[] args) {
        Map<Integer, Row> transactions = new HashMap<>();
        File f = new File("u.data");
        try (Scanner scanner = new Scanner(f)) {
            while (scanner.hasNextLine()) {
                String str = scanner.nextLine();
                String[] arr = str.split("\t");
                if (arr.length != 4) {
                    throw new IllegalArgumentException();
                } else {
                    int user = Integer.parseInt(arr[0]);
                    int item = Integer.parseInt(arr[1]);
                    if (transactions.containsKey(user)) {
                        List<Integer> l = transactions.get(user).getAs(0);
                        l.add(item);
                    } else {
                        List<Integer> items = new ArrayList();
                        items.add(item);
                        transactions.put(user, RowFactory.create(items));
                    }
                }
            }
            SparkUtil spark = SparkUtil.build("application.properties");
            SparkSession session = spark.getSession();
            StructType schema = new StructType(new StructField[]{new StructField(
                "items", new ArrayType(DataTypes.IntegerType, true), false, Metadata.empty())
            });
            Dataset<Row> transactionDF = session.createDataFrame(new LinkedList(transactions.values()), schema);
            FPGrowthModel model = new FPGrowth()
                    .setItemsCol("items")
                    .setMinSupport(0.15)
                    .setMinConfidence(0.8)
                    .fit(transactionDF);
            Dataset rules = model.associationRules();
            Iterator iterator = rules.toLocalIterator();
            f = new File("out.txt");
            try (PrintWriter printWriter = new PrintWriter(f)) {
                printWriter.write("");
                while (iterator.hasNext()) {
                    Row rule = (Row) iterator.next();
                    List<String> antecedent = rule.getList(0);
                    List<Integer> consequent = rule.getList(1);
                    double confidence = rule.getDouble(2);
                    Iterator it = antecedent.iterator();
                    printWriter.append("[");
                    while (it.hasNext()) {
                        printWriter.append(it.next().toString());
                        if (it.hasNext()) {
                            printWriter.append(", ");
                        }
                    }
                    printWriter.append("]");
                    printWriter.append(" => ");
                    printWriter.append(consequent.get(0).toString());
                    printWriter.append(", confidence: " + confidence + "\n");
                }
            } catch (FileNotFoundException ex) {
                Logger.getLogger(AssociationRule.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (FileNotFoundException ex) {
            Logger.getLogger(AssociationRule.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
