package com.movie.script.analysis;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CharacterWordMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Text characterWord = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
         // Convert the input line to a String
         String line = value.toString();
        
         // Assuming the format is: "CHARACTER: DIALOGUE"
         int separatorIndex = line.indexOf(':');
         
         if (separatorIndex != -1) {
             // Extract the character's name and dialogue
             String characterName = line.substring(0, separatorIndex).trim();
             String dialogue = line.substring(separatorIndex + 1).trim();
             
             // Tokenize the dialogue to get individual words
             StringTokenizer itr = new StringTokenizer(dialogue);
             
             while (itr.hasMoreTokens()) {
                 // Get each word from the dialogue
                 String word = itr.nextToken().toLowerCase(); // Convert to lower case for consistency
                 
                 // Create a composite key with character's name and the word
                 characterWord.set(characterName + ":" + word);
                 
                 // Emit the composite key and the count (1)
                 context.write(characterWord, one);
             }
         }
     }
    
}
