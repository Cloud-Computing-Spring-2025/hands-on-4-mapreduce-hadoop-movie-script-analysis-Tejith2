package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class CounterMapper extends Mapper<Object, Text, Text, IntWritable> {

    private static final Text LINE_COUNT = new Text("Total Lines Processed");
    private static final Text WORD_COUNT = new Text("Total Words Processed");
    private static final Text CHARACTER_COUNT = new Text("Total Characters Processed");
    private static final Text UNIQUE_WORD_COUNT = new Text("Total Unique Words Identified");
    private static final Text SPEAKING_CHARACTERS = new Text("Number of Characters Speaking");

    private IntWritable one = new IntWritable(1);
    private IntWritable wordCount = new IntWritable();
    private IntWritable charCount = new IntWritable();
    
    private HashSet<String> uniqueWords = new HashSet<>();
    private HashSet<String> charactersSpeaking = new HashSet<>();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (line.isEmpty() || !line.contains(":")) return; // Ignore invalid lines

        String[] parts = line.split(":", 2);
        if (parts.length < 2) return;

        String character = parts[0].trim();
        String dialogue = parts[1].trim();

        // Track speaking characters
        charactersSpeaking.add(character);

        // Count words and characters
        StringTokenizer tokenizer = new StringTokenizer(dialogue);
        int words = 0;
        int chars = dialogue.length();

        while (tokenizer.hasMoreTokens()) {
            String word = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!word.isEmpty()) {
                uniqueWords.add(word);
                words++;
            }
        }

        // Emit statistics
        context.write(LINE_COUNT, one);
        wordCount.set(words);
        charCount.set(chars);
        context.write(WORD_COUNT, wordCount);
        context.write(CHARACTER_COUNT, charCount);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        context.write(UNIQUE_WORD_COUNT, new IntWritable(uniqueWords.size()));
        context.write(SPEAKING_CHARACTERS, new IntWritable(charactersSpeaking.size()));
    }
}
