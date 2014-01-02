/**
 * 
 */
package org.apache.hadoop.io.compress.tools;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * @author howie_yu
 * 
 */
public class CommandHandler {

	public static final String AESKEY = "aeskey";

	public static final String FILE_OUT_PATH = "out";

	public static final String FILE_IN_PAYH = "in";

	public static CommandLine parser(String[] args) throws ParseException {

		// create Options object
		Options options = new Options();

		// add t option
		options.addOption(new Option("help", "print this message"));
		options.addOption(AESKEY, true, "AES key");
		options.addOption(FILE_OUT_PATH, true, "Output path");
		options.addOption(FILE_IN_PAYH, true, "Input path");
		options.addOption("e", false, "Encryption");
		options.addOption("d", false, "Decryption");

		// create the parser
		CommandLineParser parser = new BasicParser();

		// parse the command line arguments
		CommandLine cmd = parser.parse(options, args);

		if(cmd.getOptions().length < 1) {
			createErrorMsgAndExit("Please input arguments:" + cmd.getOptions().length);
		}
		if(cmd.hasOption("help") || cmd.hasOption("h")) {
			// automatically generate the help statement
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("Tools", options);
			System.exit(-1);
		}

		if(!cmd.hasOption(AESKEY)) {
			createErrorMsgAndExit("Please input ase key");
		}

		if(!cmd.hasOption(FILE_OUT_PATH)) {
			createErrorMsgAndExit("Please input output path");
		}
		if(!cmd.hasOption(FILE_IN_PAYH)) {
			createErrorMsgAndExit("Please input input path");
		}
		if(!cmd.hasOption("e") && !cmd.hasOption("d")) {
			createErrorMsgAndExit("Please chooes action encryption or decryption");
		}

		return cmd;
	}

	private static void createErrorMsgAndExit(String errorMsg) {
		System.err.println(errorMsg);
		System.exit(-1);
	}

}
