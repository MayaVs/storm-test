package com.detica.cyberreveal.storm.bolt;

import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * a Storm Bolt which writes
 *      all received words and their count,
 *      the total count of all words,
 *      the number of all words,
 *      the number of different words,
 *      the most common word and count (not counting conjunctions, articles, prepositions and more filtered by notImportantWords list)
 * to console
 */
public class TotalPrinterBolt extends BaseBasicBolt {

    private final Map<String, Long> wordCounts = new HashMap<String, Long>();
    Long allWordsCount = 0L;
    Long mostUsedWordCount = 0L;
    String mostUsedWord = "";
    List<String>  notImportantWords;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //don't want to count words like "the for on..." so added them in a list to skip
        //this list is set while configuring the topology
        this.notImportantWords = Arrays.asList(stormConf.get("notImportantWords").toString().split(" "));
    }

    public TotalPrinterBolt(){
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        String tupleWord = tuple.getStringByField("word");
        Long tupleCount = tuple.getLongByField("count");

        Long currentWordCount = wordCounts.containsKey(tupleWord)?wordCounts.get(tupleWord):0;

        //check if tupleWord is the most used word
        if(!this.notImportantWords.contains(tupleWord)){
            if(tupleCount > this.mostUsedWordCount){
                this.mostUsedWordCount = tupleCount;
                this.mostUsedWord = tupleWord;
            }
        }

        if(tupleCount > currentWordCount) {
            this.wordCounts.put(tupleWord, tupleCount); //add tupleCount value to the map
            this.allWordsCount +=  tupleCount - currentWordCount; //update the count of all words
        }

   }

    @Override
    public void cleanup() {
        /* to do
         move this printing in execute() and add timer logic to print totals for every X ms
         */

        System.out.println("--- FINAL COUNTS ---");
        List<String> keys = new ArrayList<String>(this.wordCounts.keySet());
        Collections.sort(keys); //sort all words from the map in alphabetical order
        for (String key : keys) {
            System.out.println(key + " : " + this.wordCounts.get(key)); //print all the words
        }

        System.out.println("The most common word (not part of notImportantWords)  is \"" + this.mostUsedWord +"\". It was used " + this.mostUsedWordCount + " times");
        System.out.println("The total number of different words is " + this.wordCounts.size());
        System.out.println("The total count of all words is " + this.allWordsCount);
        System.out.println("--------------");
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer ofd) {
        //no output for this bolt
    }

}
