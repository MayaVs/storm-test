package com.detica.cyberreveal.storm.bolt;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * a Storm Bolt which appends  received lines that contain any word of a filter list to a specified file.
 */
public class FilePrintLinesWithFilterWordsBolt extends BaseBasicBolt {

    File outputFile;
    String[] crimeWords;
    FileWriter writer;


    /**
     * Instantiates a new file printer bolt.
     *
     * @param outputFile
     *            the output file
     */
    public FilePrintLinesWithFilterWordsBolt(final File outputFile){
        this.outputFile = outputFile;
    }

    @Override
    public void cleanup() {
        //closing the file writer only once here when closing the topology
        try {
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        //initialise the file writer only once here when initialising the bolt
        try {
            this.writer = new FileWriter(this.outputFile, true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.crimeWords = stormConf.get("crimeWords").toString().split(" ");
    }

    @Override
    public void execute(final Tuple tuple, final BasicOutputCollector collector) {
        String line = tuple.getStringByField("line");
        try {
            for (String word : this.crimeWords)
            {
                //if the line contains any of the words in the list it is added to the output file
                if(line.contains(word)){
                    writer.append(line + "\n");
                    break;
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(final OutputFieldsDeclarer ofd) {
        //no output for this bolt
    }

}
