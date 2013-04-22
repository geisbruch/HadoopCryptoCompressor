package org.apache.hadoop.io.compress.crypto;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.hadoop.io.compress.Decompressor;

import sec.util.Crypto;

public class CryptoBasicDecompressor implements Decompressor {

	Crypto crypto;
	
	ByteBuffer in;
	

	private boolean finish = false;

	private boolean finished = false;

	private boolean dataSet = false;

	
	private ByteBuffer remain;
	
	public CryptoBasicDecompressor(String key){
		crypto = new Crypto(key);
	}
	
	@Override
	public synchronized int decompress(byte[] buf, int off, int len) throws IOException {
		finished  = false;
		
		//If no dataset ones is needed
		if(!dataSet){
			dataSet=true;
			return 0;
		}
		
		//If there are data remaining
		if(remain != null && remain.remaining()>0){
			int size = Math.min(buf.length, remain.remaining());
			remain.get(buf, off, size);
			return size;
		}
		
		dataSet = false;
		
		//If no data in remaining
		if(in != null && in.remaining()<=0){
			finished = true;
			return 0;
		}
		
		//Standar case
		byte[] w = new byte[in.remaining()];
		in.get(w,0,in.remaining());
		byte[] b = crypto.decrypt(w);
		remain = ByteBuffer.wrap(b);
		int size = Math.min(buf.length-off, remain.remaining());
		remain.get(buf, off, size);
		if(remain.remaining()<=0)
			finished  = true;
		return size;

	}

	@Override
	public void end() {

	}



	@Override
	public boolean finished() {
		return finished;
	}



	@Override
	public boolean needsInput() {
		return dataSet && (remain == null || remain.remaining()<=0);
	}


	@Override
	public void reset() {
		in = null;
		finish = false;
		finished = false;
		dataSet = false;
		remain = null;
		
	}

	@Override
	public void setDictionary(byte[] arg0, int arg1, int arg2) {
	}

	@Override
	public synchronized void setInput(byte[] buf, int offset, int length) {
		dataSet  = true;
		in = ByteBuffer.wrap(buf, offset, length);
		finished  = false;
	}

	@Override
	public boolean needsDictionary() {
		return false;
	}

	@Override
	public int getRemaining() {
		return 0;
	}
}
