package org.apache.hadoop.io.compress.crypto;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;

public class CyptoCodec implements CompressionCodec, Configurable {
	
	public static final String CRYPTO_DEFAULT_EXT = ".cypto";
	public static final String CRYPTO_SECRET_KEY = "cypto.secret.key";
	private Configuration config;
	@Override
	public Compressor createCompressor() {
		return new CryptoBasicCompressor(config.get(CRYPTO_SECRET_KEY));
	}

	@Override
	public Decompressor createDecompressor() {
		return new CryptoBasicDecompressor(config.get(CRYPTO_SECRET_KEY));
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in)
			throws IOException {
		return createInputStream(in, createDecompressor());
	}

	@Override
	public CompressionInputStream createInputStream(InputStream in,
			Decompressor decomp) throws IOException {
		return new BlockDecompressorStream(in, decomp);
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out)
			throws IOException {
		return createOutputStream(out, createCompressor());
	}

	@Override
	public CompressionOutputStream createOutputStream(OutputStream out,
			Compressor comp) throws IOException {
		return new BlockCompressorStream(out, comp);
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
