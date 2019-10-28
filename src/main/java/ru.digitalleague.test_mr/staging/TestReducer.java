package ru.digitalleague.test_mr.staging;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import javax.naming.Context;
import java.io.IOException;
import java.util.Arrays;

public class TestReducer extends Reducer<Text, Text, Text, Text> {
//public class TestReducer extends Reducer<Text, IntWritable, LongWritable, Text> {

    String OUTPUT_DELIMITER = ";";

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int[] array = {0,0,0,0,0,0,0,0};
        String[] splittedValue = new String[]{};
        for (Text value : values) {
            splittedValue = value.toString().split(";");

            for(int i=0; i<splittedValue.length; i++) {
                array[i] += Integer.parseInt(splittedValue[i]);
            }
        }

        String[] splittedKey = key.toString().split(";");

        VALUE.set(new Text(
                String.join(OUTPUT_DELIMITER, splittedKey[1], Arrays.toString(array)
                        .replaceAll("\\s+", "")
                        .replaceAll("\\[", "")
                        .replaceAll("\\]","")
                        .replaceAll(",", ";"))));
        KEY.set(splittedKey[0]);
        context.write(KEY, VALUE);
    }
}
