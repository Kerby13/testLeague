package ru.digitalleague.test_mr.staging.Stage3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SecondResultMapper extends Mapper<Text, Text, Text, Text> {
    private String INPUT_DELIMITER = ";";
    private String STRING_DELIMITER = "    ";
    private int PHONE = 0;
    private int NAME = 1;
    private static String FILE_TAG = "2nd";

    int MIN_LENGTH = 2;

    Text KEY = new Text();
    Text VALUE = new Text();

    @Override
    protected void map(Text key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] splittedValue = value.toString().split(INPUT_DELIMITER);
        if (splittedValue.length >= MIN_LENGTH) {
            String phone = splittedValue[PHONE];
            String other = splittedValue[NAME];

            KEY.set(phone);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, other));

            context.write(KEY, VALUE);
        }
    }
}
