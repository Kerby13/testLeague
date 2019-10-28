package ru.digitalleague.test_mr.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.digitalleague.test_mr.staging.TestMapper;
import ru.digitalleague.test_mr.staging.TestReducer;

public class TestMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TestMain(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        ProjectParams params = new ProjectParams(args);


        Job job = Job.getInstance(params.config);
        job.setJobName(params.projectName);

        job.setJarByClass(TestMain.class);

        job.setNumReduceTasks(params.reducesCnt);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(TestMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(TestReducer.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(job.getConfiguration());

        FileInputFormat.addInputPath(job, new Path(params.inputTestPath));
        FileOutputFormat.setOutputPath(job, new Path(params.outputTestPath));

        if (hdfs.exists(new Path(params.outputTestPath))) {
            hdfs.delete(new Path(params.outputTestPath), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}

