/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.kien.sparkmlexample;

import com.kien.sparkmlexample.domain.Rating;
import com.kien.sparkmlexample.utils.SparkUtil;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;

/**
 *
 * @author KienNT
 */
public class CollaborativeFiltering {

    private Dataset<Row> createDataframe(String filePath, Class clazz) {
        SparkSession session = SparkUtil.build("application.properties").getSession();
        JavaRDD<Rating> ratingsRDD = session
                .read().textFile(filePath).javaRDD()
                .map((String str) -> Rating.parseRating100k(str));
        return session.createDataFrame(ratingsRDD, clazz);
    }

    private ALSModel getModel(Dataset<Row> training) {
        ALS als = new ALS()
                .setMaxIter(5)
                .setRegParam(0.01)
                .setUserCol("user")
                .setItemCol("item")
                .setRatingCol("rating");
        return als.fit(training);
    }

    public List<Integer> recommend(Integer user, int numberItem) {
        Dataset<Row> ratings = createDataframe("u.data", Rating.class);
        ALSModel model = getModel(ratings);
        Dataset<Row> result = model.recommendForAllUsers(numberItem);
        Iterator iterator = result.where("user = " + user).toLocalIterator();
        List<Integer> items = new ArrayList();
        if (iterator.hasNext()) {
            Row r = (Row) iterator.next();
            List<GenericRowWithSchema> list = r.getList(1);
            for (GenericRowWithSchema row : list) {
                items.add(row.getInt(0));
            }
        }
        return items;
    }

    public static void test() {
        CollaborativeFiltering service = new CollaborativeFiltering();
        Dataset<Row> ratings = service.createDataframe("u.data", Rating.class);
        Dataset<Row>[] splits = ratings.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> training = splits[0];
        Dataset<Row> test = splits[1];

        ALSModel model = service.getModel(training);
        model.setColdStartStrategy("drop");
        Dataset<Row> predictions = model.transform(test);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol("rating")
                .setPredictionCol("prediction");
        Double rmse = evaluator.evaluate(predictions);
        System.out.println("Root-mean-square error = " + rmse);
    }

    public static void main(String[] args) {
        test();
    }
}
