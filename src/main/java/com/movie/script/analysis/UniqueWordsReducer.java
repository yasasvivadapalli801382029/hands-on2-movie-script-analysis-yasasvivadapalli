package com.movie.script.analysis;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;

public class UniqueWordsReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        HashSet<String> uniqueWords = new HashSet<>();
        
        // Iterate over the values (words) and add them to the HashSet
        for (Text value : values) {
            uniqueWords.add(value.toString());
        }
        
        // Convert the HashSet to a string with words separated by commas
        StringBuilder sb = new StringBuilder();
        for (String word : uniqueWords) {
            if (sb.length() > 0) {
                sb.append(", ");
            }
            sb.append(word);
        }
        
        // Write the character's name and the list of unique words to the context
        context.write(key, new Text(sb.toString()));
    }

}

