package ru.digitalleague.test_mr.staging.tools;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.types.Pair;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class IOHelper {
    public List<Pair<LongWritable, Text>> read(String fileName) {

        List<Pair<LongWritable, Text>> result =
                new ArrayList<Pair<LongWritable, Text>>();
        try {
            //
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null) {
                result.add(new Pair<LongWritable, Text>(new LongWritable(0), new Text(line)));
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public <K, V> void write(String fileName, List<Pair<K, V>> output) {
        try {
            //
            BufferedWriter br = new BufferedWriter(new FileWriter(fileName));
            String line ="";
            for (Pair pair : output) {
                line += pair.getFirst().toString() + ";" + pair.getSecond().toString() + "\n";//pair.getSecond().toString() + "\n";
            }
            br.write(line);
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
