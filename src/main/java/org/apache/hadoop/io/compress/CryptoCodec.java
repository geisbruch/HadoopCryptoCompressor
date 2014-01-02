package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.crypto.CryptoBasicCompressor;
import org.apache.hadoop.io.compress.crypto.CryptoBasicDecompressor;

/**
 * This Crypt Codec enable you to create crypto files using this codec as a compressor
 * so you can use encripted files on hadoop
 * 
 * @author geisbruch
 * 
 */
public class CryptoCodec implements CompressionCodec, Configurable {

	private static final Log LOG = LogFactory.getLog(CryptoCodec.class);

	public static final String CRYPTO_DEFAULT_EXT = ".crypto";

	public static final String CRYPTO_SECRET_KEY = "crypto.secret.key";

	private Configuration config;

	@Override
	public Compressor createCompressor() {
		LOG.info("Creating compressor");
		return new CryptoBasicCompressor(config.get(CRYPTO_SECRET_KEY));
	}

	@Override
	public Decompressor createDecompressor() {
		LOG.info("Creating decompressor");
		return new CryptoBasicDecompressor(config.get(CRYPTO_SECRET_KEY));
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in) throws IOException {
		return createInputStream(in, createDecompressor());
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in, Decompressor decomp) throws IOException {
		LOG.info("Creating DecompressorStream stream");
		return new DecompressorStream(in, decomp);
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out) throws IOException {
		return createOutputStream(out, createCompressor());
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out, Compressor comp) throws IOException {
		LOG.info("Creating output stream");
		return new CompressorStream(out, comp);
	}

	@Override
	public Class<? extends Compressor> getCompressorType() {
		return CryptoBasicCompressor.class;
	}

	@Override
	public Class<? extends Decompressor> getDecompressorType() {
		return CryptoBasicDecompressor.class;
	}

	@Override
	public String getDefaultExtension() {
		return CRYPTO_DEFAULT_EXT;
	}

	@Override
	public Configuration getConf() {
		return this.config;
	}

	@Override
	public void setConf(Configuration config) {
		this.config = config;
	}

}
