package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CharacterWordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;

    // Iterate through all values (counts) associated with the key
    for (IntWritable value : values) {
        sum += value.get();  // Accumulate the count
    }

    // Write the result (character and total word count) to the context
    context.write(key, new IntWritable(sum));

    }
    
}
