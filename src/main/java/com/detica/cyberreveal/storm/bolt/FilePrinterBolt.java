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
 * a Storm Bolt which appends all received tuples to a specified file.
 */
public class FilePrinterBolt extends BaseBasicBolt {

	private static final long serialVersionUID = 327323938874334973L;
	File outputFile;
	FileWriter writer;

	/**
	 * Instantiates a new file printer bolt.
	 * 
	 * @param outputFile
	 *            the output file
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public FilePrinterBolt(final File outputFile) throws IOException {
		this.outputFile = outputFile;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		//initialise the writer only once here when bolt is initialised
		try {
			writer = new FileWriter(this.outputFile, true);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void cleanup() {
		//close the writer only once when the bolt is about to shut down
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void execute(final Tuple tuple, final BasicOutputCollector collector) {
		try {
			writer.append(tuple.toString() + "\n");
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void declareOutputFields(final OutputFieldsDeclarer ofd) {
	}

}
