package com.detica.cyberreveal.storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * a Storm Bolt which receives words with count number, matches them to specific words in a list and appends to a specified output file.
 * Can be used to track specific words
 */
public class MatchWordsBolt extends BaseBasicBolt {

    Set<String> matchWordsSet;

    public MatchWordsBolt(){
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        this.matchWordsSet = new HashSet<String>(Arrays.asList(stormConf.get("matcherWords").toString().split(" ")));
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        String tupleWord = tuple.getStringByField("word");
        Long tupleCount = tuple.getLongByField("count");
        if(matchWordsSet.contains(tupleWord)){
            collector.emit(new Values(tupleWord, tupleCount));
        }
   }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer ofd) {
        ofd.declare(new Fields("word", "count"));
    }
}
