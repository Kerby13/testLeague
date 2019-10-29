package ru.digitalleague.test_mr.staging.Stage1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ActivityMapper extends Mapper<LongWritable, Text, Text, Text> {
    private String INPUT_DELIMITER = "\\|";
    private String STRING_DELIMITER = ";";
    private int PHONE_NUMBER = 1;
    private int DATETIME = 2;
    private int ACTIVITY_TYPE = 32;

    int MIN_LENGTH = 33;

    Text KEY = new Text();
    //IntWritable VALUE = new IntWritable();
    Text VALUE = new Text();


    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] splittedValue = value.toString().split(INPUT_DELIMITER);

        if (splittedValue.length >= MIN_LENGTH) {
            String phone_number = splittedValue[PHONE_NUMBER];
            String date = splittedValue[DATETIME].substring(0, 8);
            String activity_type = splittedValue[ACTIVITY_TYPE];


            int time = Integer.parseInt(splittedValue[DATETIME].substring(8, 10));
            if ((activity_type.equals("V")) && (time >= 6 && time < 11)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "1", "0", "0", "0", "0", "0", "0", "0"));
            } else if ((activity_type.equals("V")) && (time >= 11 && time < 19)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "1", "0", "0", "0", "0", "0", "0"));
            } else if ((activity_type.equals("V")) && (time >= 19 && time < 23)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "1", "0", "0", "0", "0", "0"));
            } else if (activity_type.equals("V")) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "0", "1", "0", "0", "0", "0"));
            } else if ((activity_type.equals("S")) && (time >= 6 && time < 11)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "0", "0", "1", "0", "0", "0"));
            } else if ((activity_type.equals("S")) && (time >= 11 && time < 19)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "0", "0", "0", "1", "0", "0"));
            } else if ((activity_type.equals("S")) && (time >= 19 && time < 23)) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "0", "0", "0", "0", "1", "0"));
            } else if (activity_type.equals("S")) {
                KEY.set(String.join(STRING_DELIMITER, phone_number, date));
                VALUE.set(String.join(STRING_DELIMITER, "0", "0", "0", "0", "0", "0", "0", "1"));
            }
            //VALUE.set(1);

            context.write(KEY, VALUE);
        }
    }
}
