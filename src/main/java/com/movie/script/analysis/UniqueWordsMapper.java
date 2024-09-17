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
        
     String line = value.toString();
        
        // Assuming the format is: "CHARACTER: DIALOGUE"
        int separatorIndex = line.indexOf(':');
        
        if (separatorIndex != -1) {
            // Extract the character's name and dialogue
            String characterName = line.substring(0, separatorIndex).trim();
            String dialogue = line.substring(separatorIndex + 1).trim();
            
            // Set the character's name as the key
            character.set(characterName);
            
            // Tokenize the dialogue to get individual words
            StringTokenizer itr = new StringTokenizer(dialogue);
            
            while (itr.hasMoreTokens()) {
                // Get each word from the dialogue
                String token = itr.nextToken().toLowerCase(); // Convert to lower case for uniqueness
                word.set(token);
                
                // Emit the character's name and the word
                context.write(character, word);
            }
        }
    }
}

    

