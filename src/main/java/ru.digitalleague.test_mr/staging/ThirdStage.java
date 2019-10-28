package ru.digitalleague.test_mr.staging;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.types.Pair;
import ru.digitalleague.test_mr.staging.tools.IOHelper;

import javax.naming.Name;

public class ThirdStage {

    public static class FirstStageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String INPUT_DELIMITER = ";";
        private String STRING_DELIMITER = "    ";
        private int PHONE = 0;
        private static String FILE_TAG = "1st";

        Text KEY = new Text();
        Text VALUE = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String[] splittedValue = value.toString().split(INPUT_DELIMITER);
            String phone = splittedValue[PHONE];
            String other = value.toString();//.replace(value.toString().substring(0, 11), "");

            KEY.set(phone);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, other));

            context.write(KEY, VALUE);
        }
    }

    public static class SecondStageMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String INPUT_DELIMITER = ";";
        private String STRING_DELIMITER = "    ";
        private int PHONE = 0;
        private int NAME = 1;
        private static String FILE_TAG = "2nd";

        Text KEY = new Text();
        Text VALUE = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String[] splittedValue = value.toString().split(INPUT_DELIMITER);
            String phone = splittedValue[PHONE];
            String other = splittedValue[NAME];

            KEY.set(phone);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, other));

            context.write(KEY, VALUE);
        }
    }


    public static class StageJoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String k = "";
            String v = "";
            for (Text t : values) {
                String parts[] = t.toString().split("    ");
                if (parts[0].equals("1st")) {
                    k = parts[1];
                } else if (parts[0].equals("2nd")) {
                    v = parts[1];
                }
            }
            if (k != "")
                if (v == "")
                    context.write(new Text(k), new Text("NULL"));
                else
                    context.write(new Text(k), new Text(v));
        }
    }

}
