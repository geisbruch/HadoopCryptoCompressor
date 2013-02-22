package org.apache.hadoop.io.compress.crypto;

import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.*;

public class FileEncryptDecrypter {
    CompressionCodec codec;
    Configuration conf = new Configuration();

    public FileEncryptDecrypter(String secret) throws ClassNotFoundException{
        conf.set(CryptoCodec.CRYPTO_SECRET_KEY, secret);
        codec = (CompressionCodec) ReflectionUtils.newInstance(conf.getClassByName(CryptoCodec.class.getName()), conf);
    }

    void decryptFile(InputStream is, OutputStream stream, int blockSize) throws IOException {
        CompressionInputStream cis = codec.createInputStream(is);
        byte[] block = new byte[blockSize];
        int len;
        int bytesWritten = 0;
        while((len = cis.read(block)) > 0) {
            stream.write(block,0,len);
            bytesWritten += len;
        }
        stream.flush();
    }

    void encryptFile(InputStream is, OutputStream stream, int blockSize) throws IOException {
        CompressionOutputStream cos = codec.createOutputStream(stream);
        byte[] block = new byte[blockSize];
        int len;
        while ((len = is.read(block)) > 0) {
            cos.write(block, 0, len);
        }
        cos.flush();
    }

    public static void main(String[] args) throws ClassNotFoundException, IOException {
        CommandLine line = null;
        try {
            line = getCommandLine(args);
        } catch (ParseException e) {
            System.exit(1);
            return;
        }

        String secret = line.getOptionValue("secret");
        String outFilename = line.getArgs().length > 1 ? line.getArgs()[1] : "-";
        String inFilename = line.getArgs().length > 0 ? line.getArgs()[0] : "-";
        int blockSize = Integer.parseInt(line.getOptionValue("blocksize", "2048"));
        FileEncryptDecrypter fed = new FileEncryptDecrypter(secret);

        OutputStream os = null;
        InputStream is = null;
        try {
            os = outFilename.equals("-") ? System.out : new FileOutputStream(outFilename);
            is = inFilename.equals("-") ? System.in : new FileInputStream(inFilename);
        } catch (FileNotFoundException e) {
            System.err.print(e.getMessage());
            System.exit(2);
            return;
        }

        try {
            if (line.hasOption("decrypt")) {
                fed.decryptFile(is, os, blockSize);
            } else if (line.hasOption("encrypt")) {
                fed.encryptFile(is, os, blockSize);
            }
        } catch (IOException e) {
            System.err.print("Error processing file: " + e.getMessage());
            System.exit(3);
            return;
        } finally {
            is.close();
            os.close();
        }
    }

    private static CommandLine getCommandLine(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("d", "decrypt", false, "Decrypt specified file");
        options.addOption("e", "encrypt", false, "Encrypt specified file");
        options.addOption("b", "blocksize", true, "Block size to use for encryption");
        Option opt = OptionBuilder.withLongOpt("secret")
                .withDescription("Secret for encryption / decryption")
                .isRequired()
                .hasArgs(1)
                .create('s');
        options.addOption(opt);

        CommandLineParser parser = new GnuParser();
        CommandLine line;
        try {
            line = parser.parse(options, args);
        } catch (ParseException e) {
            System.err.print("Could not parse command line: " + e.getMessage());
            throw e;
        }

        if (!(line.hasOption("decrypt") ^ line.hasOption("encrypt"))) {
            System.err.print("You must provide the decrypt OR encrypt option");
            throw new ParseException("Invalid options.");
        }

        return line;
    }
}
