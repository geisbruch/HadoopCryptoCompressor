package org.apache.hadoop.io.compress.tools;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import sec.util.Crypto;

public class Tools {

	private static final Log logger = LogFactory.getLog(Tools.class);

	public static void main(String[] args) {

		try {
			CommandLine cmd = CommandHandler.parser(args);

			String inputPath = cmd.getOptionValue(CommandHandler.FILE_IN_PAYH);
			String outPath = cmd.getOptionValue(CommandHandler.FILE_OUT_PATH);
			String aesKey = cmd.getOptionValue(CommandHandler.AESKEY);
			Crypto crypto = new Crypto(aesKey);

			FileInputStream fin = new FileInputStream(new File(inputPath));
			FileOutputStream fout = new FileOutputStream(new File(outPath));

			if(cmd.hasOption("e")) {

				logger.info("Star encrypt file:" + inputPath + " to " + outPath);
				crypto.encrypt(fin, fout);
			}
			else if(cmd.hasOption("d")) {

				logger.info("Star decrypt file:" + inputPath + " to " + outPath);
				crypto.decrypt(fin, fout);
			}

			fin.close();
			fout.close();
		}
		catch(ParseException e) {
			logger.error("", e);
			System.exit(-1);
		}
		catch(FileNotFoundException e) {
			logger.error("", e);
			System.exit(-1);
		}
		catch(IOException e) {
			logger.error("", e);
			System.exit(-1);
		}

	}
}
