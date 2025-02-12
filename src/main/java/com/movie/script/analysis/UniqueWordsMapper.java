package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class UniqueWordsMapper extends Mapper<Object, Text, Text, Text> {

    private Text character = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        if (!line.contains(":")) return;

        String[] parts = line.split(":", 2);
        if (parts.length < 2) return;

        character.set(parts[0].trim());
        HashSet<String> uniqueWords = new HashSet<>();

        StringTokenizer tokenizer = new StringTokenizer(parts[1].trim());
        while (tokenizer.hasMoreTokens()) {
            String cleanWord = tokenizer.nextToken().replaceAll("[^a-zA-Z]", "").toLowerCase();
            if (!cleanWord.isEmpty()) {
                uniqueWords.add(cleanWord);
            }
        }

        for (String w : uniqueWords) {
            word.set(w);
            context.write(character, word);
        }
    }
}
