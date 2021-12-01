package com.detica.cyberreveal.storm;

import java.io.File;
import java.io.IOException;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;

import com.detica.cyberreveal.storm.topology.BookTopology;

/**
 * Main class. This should be used only for testing purposes.
 */
public final class Main {

	/**
	 * Private Constructor. this is a utility class and should not be
	 * instantiated.
	 */
	private Main() {
		// Do Nothing
	}

	/**
	 * The main method.
	 * 
	 * @param args
	 *            the arguments
	 * @throws AlreadyAliveException
	 *             the already alive exception
	 * @throws InvalidTopologyException
	 *             the invalid topology exception
	 * @throws IOException
	 *             Signals that an I/O exception has occurred.
	 */
	public static void main(final String[] args) throws AlreadyAliveException,
			InvalidTopologyException, IOException {
		String topologyName = null;

		if (args != null && args.length > 0) {
			topologyName = args[0];
		}
		BookTopology topology = new BookTopology(topologyName,
				"src/main/resources/AdventuresOfSherlockHolmes.txt", //file to read input from
				new File("target/wordCounts.out"), //file to write all words and how many times are counted in the text live during the topology runs
				new File("target/filterLinesWithCrimeWords.out")); //file to write lines that contain special crime words from filter list

			topology.run();
	}
}
