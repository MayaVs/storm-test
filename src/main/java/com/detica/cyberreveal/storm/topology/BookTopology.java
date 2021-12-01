package com.detica.cyberreveal.storm.topology;

import java.io.File;
import java.io.IOException;

import com.detica.cyberreveal.storm.bolt.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

import com.detica.cyberreveal.storm.spout.BookLineSpout;

import backtype.storm.tuple.Fields;

/**
 * The Class BookTopology.
 */
public final class BookTopology implements Runnable {

    private final String topologyName;
    private static final Logger LOG = LoggerFactory
            .getLogger(BookTopology.class);
    private final String inputFile;
    private final TopologyBuilder builder;
    private final Config config;
    private final File wordCountOutputFile;
    private final File filterLinesWithCrimeWords;

    /**
     * Instantiates a new book topology.
     *
     * @param topologyName              the topology name
     * @param inputFile                 name for file for input data
     * @param wordCountOutputFile       file to write all words and counts live while the topology runs.
     * @param filterLinesWithCrimeWords file to write lines containing crime words from a filter list
     */
    public BookTopology(String topologyName, String inputFile,
                        File wordCountOutputFile, File filterLinesWithCrimeWords) {
        this.topologyName = topologyName;
        this.inputFile = inputFile;
        this.wordCountOutputFile = wordCountOutputFile;
        this.filterLinesWithCrimeWords = filterLinesWithCrimeWords;


        builder = new TopologyBuilder(); //initialise the builder
        prepareTopology(); //create the builder

        config = configureTopology(); //set the configurator
    }

    //set the topology builder
    private void prepareTopology() {
        builder.setSpout("line", new BookLineSpout(), 1); //don't want to read the file twice so only 1 parallelism

        builder.setBolt("wordSplitter", new WordSplitBolt(), 2)
                .shuffleGrouping("line");

        //need to count each word only once in the same task, so use fieldsGrouping
        builder.setBolt("wordCount", new WordCountBolt(), 2)
                .fieldsGrouping("wordSplitter", new Fields("word"));

        builder.setBolt("printWordCount", new PrinterBolt(), 2)
                .fieldsGrouping("wordCount", new Fields("word"));
        try {
            builder.setBolt("printWordCountToFile",
                            new FilePrinterBolt(this.wordCountOutputFile), 2)
                    .fieldsGrouping("wordCount", new Fields("word"));
        } catch (IOException e) {
            e.printStackTrace();
        }

        builder.setBolt("printLinesWithFilterWordsToFile",
                        new FilePrintLinesWithFilterWordsBolt(this.filterLinesWithCrimeWords), 2) //
                .shuffleGrouping("line");

        builder.setBolt("matchWords",
                        new MatchWordsBolt(), 2)
                .fieldsGrouping("wordCount", new Fields("word"));

        builder.setBolt("printMatchedWordCount", new PrinterBolt(), 2)
                .fieldsGrouping("matchWords", new Fields("word"));

        //need only 1 parallelism to collect all words and sort them
        builder.setBolt("totalPrintWordCount",
                        new TotalPrinterBolt(), 1)
                .shuffleGrouping("wordCount");

    }

    private Config configureTopology() {
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("inputFile", this.inputFile);
        //words to skip when finding the most common word
        String notImportantWords = "the a s or on with for and i to of in that it you he was his is my";
        conf.put("notImportantWords", notImportantWords);
        //crime related words
        String crimeWords = "knife gun murder poison dead attack";
        conf.put("crimeWords", crimeWords);
        //specific words to match
        String matcherWords = "sherlock holmes dead house watson bohemia banker";
        conf.put("matcherWords", matcherWords);
        return conf;
    }

    @Override
    public void run() {
        if (this.topologyName != null) {
            config.setNumWorkers(20);
            try {
                StormSubmitter.submitTopology(this.topologyName, config,
                        builder.createTopology());
            } catch (AlreadyAliveException e) {
                LOG.error("Error submitting topology, topology is already existing", e);
            } catch (InvalidTopologyException e) {
                LOG.error("Error submitting topology, the topology is invalid", e);
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Utils.sleep(30000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
