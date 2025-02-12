package com.movie.script.analysis;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieScriptAnalysis {

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: MovieScriptAnalysis <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();

        // Task 1: Most Frequent Words by Character
        Job job1 = Job.getInstance(conf, "Most Frequent Words by Character");
        job1.setJarByClass(MovieScriptAnalysis.class);
        job1.setMapperClass(CharacterWordMapper.class);
        job1.setReducerClass(CharacterWordReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[1]));
        FileOutputFormat.setOutputPath(job1, new Path(args[2] + "/task1"));
        job1.waitForCompletion(true);

        // Task 2: Dialogue Length Analysis
        Job job2 = Job.getInstance(conf, "Dialogue Length Analysis");
        job2.setJarByClass(MovieScriptAnalysis.class);
        job2.setMapperClass(DialogueLengthMapper.class);
        job2.setReducerClass(DialogueLengthReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2] + "/task2"));
        job2.waitForCompletion(true);

        // Task 3: Unique Words by Character
        Job job3 = Job.getInstance(conf, "Unique Words by Character");
        job3.setJarByClass(MovieScriptAnalysis.class);
        job3.setMapperClass(UniqueWordsMapper.class);
        job3.setReducerClass(UniqueWordsReducer.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path(args[1]));
        FileOutputFormat.setOutputPath(job3, new Path(args[2] + "/task3"));
        job3.waitForCompletion(true);

        // Task 4: Store Hadoop Counters as Output
        Job job4 = Job.getInstance(conf, "Hadoop Counters");
        job4.setJarByClass(MovieScriptAnalysis.class);
        job4.setMapperClass(CounterMapper.class);
        job4.setReducerClass(CounterReducer.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job4, new Path(args[1]));
        FileOutputFormat.setOutputPath(job4, new Path(args[2] + "/task4"));
        System.exit(job4.waitForCompletion(true) ? 0 : 1);
    }
}
