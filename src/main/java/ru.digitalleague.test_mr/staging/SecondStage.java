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

public class SecondStage {

    public static class NameMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String INPUT_DELIMITER = "\u0001";
        private String STRING_DELIMITER = "    ";
        private int BAN_KEY = 0;
        private int FULL_NAME = 14;
        private static String FILE_TAG = "name";

        Text KEY = new Text();
        Text VALUE = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String[] splittedValue = value.toString().split(INPUT_DELIMITER);
            String ban = splittedValue[BAN_KEY];
            String name = splittedValue[FULL_NAME];

            KEY.set(ban);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, name));

            context.write(KEY, VALUE);
        }
    }

    public static class SubscriberMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String INPUT_DELIMITER = "\u0001";
        private String STRING_DELIMITER = "    ";
        private int BAN_KEY = 0;
        private int PHONE_NUMBER = 1;
        private static String FILE_TAG = "phone";

        Text KEY = new Text();
        Text VALUE = new Text();

        @Override
        protected void map(LongWritable key, Text value, Mapper.Context context)
                throws IOException, InterruptedException {
            String[] splittedValue = value.toString().split(INPUT_DELIMITER);
            String ban = splittedValue[BAN_KEY];
            String phone_number = "";
            if (splittedValue[PHONE_NUMBER] != "")
                phone_number = splittedValue[PHONE_NUMBER];

            KEY.set(ban);
            VALUE.set(String.join(STRING_DELIMITER, FILE_TAG, phone_number));

            context.write(KEY, VALUE);
        }
    }

    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String name = "";
            String phone = "";
            for (Text t : values) {
                String parts[] = t.toString().split("    ");
                if (parts[0].equals("phone")) {
                    phone = parts[1];
                } else if (parts[0].equals("name")) {
                    name = parts[1];
                }
            }
            if (phone != "")
                context.write(new Text(phone), new Text(name));
        }
    }

}
