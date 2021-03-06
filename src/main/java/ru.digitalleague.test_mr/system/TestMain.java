package ru.digitalleague.test_mr.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import ru.digitalleague.test_mr.staging.Stage1.ActivityMapper;
import ru.digitalleague.test_mr.staging.Stage1.FirstStageReducer;
import ru.digitalleague.test_mr.staging.Stage2.NameMapper;
import ru.digitalleague.test_mr.staging.Stage2.PhoneMapper;
import ru.digitalleague.test_mr.staging.Stage2.SecondStageReducer;
import ru.digitalleague.test_mr.staging.Stage3.FirstResultMapper;
import ru.digitalleague.test_mr.staging.Stage3.SecondResultMapper;
import ru.digitalleague.test_mr.staging.Stage3.ThirdStageReducer;

public class TestMain extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new TestMain(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {

        ProjectParams params = new ProjectParams(args);

        Job job = Job.getInstance(params.config);

        job.getConfiguration().set("mapreduce.job.queuename", params.queue_name);

        job.setJobName(params.projectName);

        job.setJarByClass(TestMain.class);

        job.setNumReduceTasks(params.reducesCnt);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem hdfs = FileSystem.get(job.getConfiguration());

        switch (params.stage_number) {
            case 1: {
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                job.setMapperClass(ActivityMapper.class);
                job.setReducerClass(FirstStageReducer.class);

                FileInputFormat.addInputPath(job, new Path(params.cmdInputPath));
                FileOutputFormat.setOutputPath(job, new Path(params.stage1_outputPath));

                if (hdfs.exists(new Path(params.stage1_outputPath))) {
                    hdfs.delete(new Path(params.stage1_outputPath), true);
                }
                break;
            }
            case 2: {
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(params.dimBanInputPath), SequenceFileInputFormat.class, NameMapper.class);
                MultipleInputs.addInputPath(job, new Path(params.subscriberInputPath), SequenceFileInputFormat.class, PhoneMapper.class);

                job.setReducerClass(SecondStageReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(params.stage2_outputPath));

                if (hdfs.exists(new Path(params.stage2_outputPath))) {
                    hdfs.delete(new Path(params.stage2_outputPath), true);
                }
                break;
            }
            case 3: {
                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(Text.class);

                MultipleInputs.addInputPath(job, new Path(params.stage1_outputPath), TextInputFormat.class, FirstResultMapper.class);
                MultipleInputs.addInputPath(job, new Path(params.stage2_outputPath), TextInputFormat.class, SecondResultMapper.class);

                job.setReducerClass(ThirdStageReducer.class);
                FileOutputFormat.setOutputPath(job, new Path(params.stage3_outputPath));

                if (hdfs.exists(new Path(params.stage3_outputPath))) {
                    hdfs.delete(new Path(params.stage3_outputPath), true);
                }
                break;
            }
        }



        /*if (hdfs.exists(new Path(params.stage1_outputPath))) {
            hdfs.delete(new Path(params.stage1_outputPath), true);
        }*/
        /*if (hdfs.exists(new Path(params.stage2_outputPath))) {
            hdfs.delete(new Path(params.stage2_outputPath), true);
        }
        if (hdfs.exists(new Path(params.stage3_outputPath))) {
            hdfs.delete(new Path(params.stage3_outputPath), true);
        }*/

        return job.waitForCompletion(true) ? 0 : 1;
    }
}