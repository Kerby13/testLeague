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
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.digitalleague.test_mr.staging.FirstStage;
import ru.digitalleague.test_mr.staging.SecondStage;
import ru.digitalleague.test_mr.staging.ThirdStage;

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

        switch (params.stage_number) {
            case 1: {
                job.setMapperClass(FirstStage.ActivityMapper.class);
                job.setReducerClass(FirstStage.ActivityReducer.class);

                FileInputFormat.addInputPath(job, new Path(params.inputTestPath1));
                FileOutputFormat.setOutputPath(job, new Path(params.outputTestPath));
            }
            case 2: {
                MultipleInputs.addInputPath(job, new Path(params.inputTestPath1), TextInputFormat.class, SecondStage.NameMapper.class);
                MultipleInputs.addInputPath(job, new Path(params.inputTestPath2), TextInputFormat.class, SecondStage.SubscriberMapper.class);

                job.setReducerClass(SecondStage.JoinReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(params.outputTestPath));
            }
            case 3: {
                MultipleInputs.addInputPath(job, new Path(params.inputTestPath1), TextInputFormat.class, ThirdStage.FirstStageMapper.class);
                MultipleInputs.addInputPath(job, new Path(params.inputTestPath2), TextInputFormat.class, ThirdStage.SecondStageMapper.class);

                job.setReducerClass(ThirdStage.StageJoinReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(params.outputTestPath));
            }
        }

        /*job.setJobName(params.projectName);

        job.setJarByClass(TestMain.class);

        job.setNumReduceTasks(params.reducesCnt);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);*/

//        job.setMapperClass(TestMapper.class);
//        job.setMapOutputKeyClass(Text.class);
//        job.setMapOutputValueClass(DoubleWritable.class);

//        job.setReducerClass(TestReducer.class);
//        job.setOutputKeyClass(NullWritable.class);
//        job.setOutputValueClass(Text.class);

        FileSystem hdfs = FileSystem.get(job.getConfiguration());

        /*FileInputFormat.addInputPath(job, new Path(params.inputTestPath));
        FileOutputFormat.setOutputPath(job, new Path(params.outputTestPath));*/

        if (hdfs.exists(new Path(params.outputTestPath))) {
            hdfs.delete(new Path(params.outputTestPath), true);
        }

        return job.waitForCompletion(true) ? 0 : 1;
    }
}