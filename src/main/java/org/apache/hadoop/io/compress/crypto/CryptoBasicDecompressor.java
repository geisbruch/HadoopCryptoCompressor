package org.apache.hadoop.io.compress.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.compress.Decompressor;

import sec.util.Crypto;

public class CryptoBasicDecompressor implements Decompressor {

	private static final Log LOG = LogFactory.getLog(CryptoBasicDecompressor.class);

	Crypto crypto;

	byte[] in;

	ByteBuffer out;

	private boolean finished = false;

	public CryptoBasicDecompressor(String key) {
		crypto = new Crypto(key);
		LOG.info("Init CryptoBasicDecompressor...");
	}

	@Override
	public synchronized int decompress(byte[] buf, int off, int len) throws IOException {
		ensureBuffer(len);

		if(out.position() >= len) {
			LOG.debug("out.position():" + out.position() + " len:" + len);
			finished = true;
		}

		if(finished && in == null) {
			LOG.debug("decompress flushBuffer....");
			return flushBuffer(buf, off, out.position());
		}

		if(needsInput()) {
			LOG.debug("needsInput....");
			return 0;
		}

		byte[] b = crypto.decrypt(in);
		in = null;
		if(b == null) {
			throw new IOException("Invalid key");
		}
		LOG.debug("decrypt:" + b.length);
		ensureBuffer(out.position() + b.length);
		out.put(b);
		return flushBuffer(buf, off, len);
	}

	private void ensureBuffer(int n) {
		if(out == null) {// Initial Allocation
			out = ByteBuffer.allocate(n * 2);
		}
		else if(out.capacity() < n) { // Grow
			ByteBuffer newBuffer = ByteBuffer.allocate(n);
			out.flip();
			newBuffer.put(out);
			out = newBuffer;
		}
	}

	@Override
	public int getRemaining() {
		return 0;
	}

	private int flushBuffer(byte[] buf, int off, int len) {
		int size = Math.min(Math.min(len, buf.length - off), out.position());
		LOG.info("flushBuffer size:" + size);
		if(size <= 0)
			return 0;
		out.flip();
		out.get(buf, off, size);
		out.compact();
		finished = true; // We don't know if there is more data for this block, but caller checks for block completion
		return size;
	}

	@Override
	public void end() {

	}

	@Override
	public boolean finished() {
		LOG.info("finished:" + finished);
		return finished;
	}

	@Override
	public boolean needsInput() {
		boolean needsInput = (in == null || in.length < 0);
		if(needsInput)
			finished = true;
		LOG.info("needsInput:" + needsInput);
		return needsInput;
	}

	@Override
	public void reset() {
		in = null;
		finished = false;
	}

	@Override
	public void setDictionary(byte[] arg0, int arg1, int arg2) {
	}

	@Override
	public synchronized void setInput(byte[] buf, int offset, int length) {
		LOG.info("setInputsize buf size" + buf.length + " length:" + length);
		if(length > 0) {
			in = new byte[length];
			int inIdx = 0;
			for(int i = offset; i < length; i++) {
				in[inIdx] = buf[i];
				inIdx++;
			}
			finished = false;
		}
	}

	@Override
	public boolean needsDictionary() {
		return false;
	}
}
