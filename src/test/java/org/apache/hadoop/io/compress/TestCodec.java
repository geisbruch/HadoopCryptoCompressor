/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.compress;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import junit.framework.Assert;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.RandomDatum;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.junit.Test;

public class TestCodec {

	private static final Log LOG = LogFactory.getLog(TestCodec.class);

	private Configuration conf = new Configuration();

	private int count = 10000;

	private int seed = new Random().nextInt();

	@Test
	public void testDecryptWithSmallerBuffer() throws IOException, ClassNotFoundException {
		codecBufferSizeTest(1024, 496);
	}

	@Test
	public void testDecryptFileWithLargerBuffer() throws IOException, ClassNotFoundException {
		codecBufferSizeTest(496, 1024);
	}

	void codecBufferSizeTest(int compressBufferSize, int decompressBufferSize)	throws IOException,
																				ClassNotFoundException {
		DataOutputBuffer data = new DataOutputBuffer();
		RandomDatum.Generator generator = new RandomDatum.Generator(seed);
		for(int i = 0; i < 20; ++i) {
			generator.next();
			RandomDatum key = generator.getKey();
			RandomDatum value = generator.getValue();

			key.write(data);
			value.write(data);
		}
		LOG.info("Generated " + count + " records");

		conf.set(CryptoCodec.CRYPTO_SECRET_KEY, "Una clave cualquiera");
		CompressionCodec c = (CompressionCodec) ReflectionUtils.newInstance(conf.getClassByName(CryptoCodec.class.getName()),
																			conf);
		// Compress data
		DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
		CompressionOutputStream os = c.createOutputStream(compressedDataBuffer);
		LOG.info("Finished compressing data");

		ByteArrayInputStream bis = new ByteArrayInputStream(data.getData());
		byte[] b = new byte[compressBufferSize];
		int len;
		while((len = bis.read(b)) > 0) {
			os.write(b, 0, len);
		}
		os.close();
		bis.reset();

		b = new byte[decompressBufferSize];
		byte[] b2 = new byte[decompressBufferSize];
		ByteArrayInputStream compressDataStream = new ByteArrayInputStream(compressedDataBuffer.getData());
		CompressionInputStream ds = c.createInputStream(compressDataStream);

		while((len = ds.read(b)) > 0) {
			int len2 = bis.read(b2, 0, len);
			for(int i = 0; i < b.length; i++) {
				Assert.assertEquals("Byte " + i + "does not match.", b[i], b2[i]);
			}
		}
		ds.close();
	}

	@Test
	public void testCryptoCodec() throws IOException {
		conf.set(CryptoCodec.CRYPTO_SECRET_KEY, "Una clave cualquiera");
		codecTest(conf, seed, 0, "org.apache.hadoop.io.compress.CryptoCodec");
		codecTest(conf, seed, count, "org.apache.hadoop.io.compress.CryptoCodec");
	}

	private static void codecTest(Configuration conf, int seed, int count, String codecClass) throws IOException {

		// Create the codec
		CompressionCodec codec = null;
		try {
			codec = (CompressionCodec) ReflectionUtils.newInstance(conf.getClassByName(codecClass), conf);
		}
		catch(ClassNotFoundException cnfe) {
			throw new IOException("Illegal codec!");
		}
		LOG.info("Created a Codec object of type: " + codecClass);

		// Generate data
		DataOutputBuffer data = new DataOutputBuffer();
		RandomDatum.Generator generator = new RandomDatum.Generator(seed);
		for(int i = 0; i < count; ++i) {
			generator.next();
			RandomDatum key = generator.getKey();
			RandomDatum value = generator.getValue();

			key.write(data);
			value.write(data);
		}
		LOG.info("Generated " + count + " records");

		// Compress data
		DataOutputBuffer compressedDataBuffer = new DataOutputBuffer();
		CompressionOutputStream deflateFilter = codec.createOutputStream(compressedDataBuffer);
		DataOutputStream deflateOut = new DataOutputStream(new BufferedOutputStream(deflateFilter));
		deflateOut.write(data.getData(), 0, data.getLength());
		deflateOut.flush();
		deflateFilter.finish();
		LOG.info("Finished compressing data");

		// De-compress data
		DataInputBuffer deCompressedDataBuffer = new DataInputBuffer();
		deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer.getLength());
		CompressionInputStream inflateFilter = codec.createInputStream(deCompressedDataBuffer);
		DataInputStream inflateIn = new DataInputStream(new BufferedInputStream(inflateFilter));
		// Check
		DataInputBuffer originalData = new DataInputBuffer();
		originalData.reset(data.getData(), 0, data.getLength());
		DataInputStream originalIn = new DataInputStream(new BufferedInputStream(originalData));
		for(int i = 0; i < count; ++i) {
			RandomDatum k1 = new RandomDatum();
			RandomDatum v1 = new RandomDatum();
			k1.readFields(originalIn);
			v1.readFields(originalIn);

			RandomDatum k2 = new RandomDatum();
			RandomDatum v2 = new RandomDatum();
			k2.readFields(inflateIn);
			v2.readFields(inflateIn);
			Assert.assertTrue(	"original and compressed-then-decompressed-output not equal",
								k1.equals(k2) && v1.equals(v2));
		}

		// De-compress data byte-at-a-time
		originalData.reset(data.getData(), 0, data.getLength());
		deCompressedDataBuffer.reset(compressedDataBuffer.getData(), 0, compressedDataBuffer.getLength());
		inflateFilter = codec.createInputStream(deCompressedDataBuffer);

		// Check
		originalIn = new DataInputStream(new BufferedInputStream(originalData));
		int expected;
		do {
			expected = originalIn.read();
			Assert.assertEquals("Inflated stream read by byte does not match", expected, inflateFilter.read());
		}
		while(expected != -1);

		LOG.info("SUCCESS! Completed checking " + count + " records");
	}

	@Test
	public void testSequenceFileCryptoCodec()	throws IOException,
												ClassNotFoundException,
												InstantiationException,
												IllegalAccessException {
		conf.set(CryptoCodec.CRYPTO_SECRET_KEY, "Una clave cualquiera");
		sequenceFileCodecTest(conf, 100, "org.apache.hadoop.io.compress.crypto.CryptoCodec", 100);
		sequenceFileCodecTest(conf, 200000, "org.apache.hadoop.io.compress.crypto.CryptoCodec", 100000);
	}

	private static void sequenceFileCodecTest(Configuration conf, int lines, String codecClass, int blockSize)	throws IOException,
																												ClassNotFoundException,
																												InstantiationException,
																												IllegalAccessException {

		Path filePath = new Path("SequenceFileCodecTest." + codecClass);
		// Configuration
		conf.setInt("io.seqfile.compress.blocksize", blockSize);

		// Create the SequenceFile
		FileSystem fs = FileSystem.get(conf);
		LOG.info("Creating SequenceFile with codec \"" + codecClass + "\"");
		SequenceFile.Writer writer = SequenceFile.createWriter(	fs,
																conf,
																filePath,
																Text.class,
																Text.class,
																CompressionType.BLOCK,
																(CompressionCodec) Class.forName(codecClass).newInstance());

		// Write some data
		LOG.info("Writing to SequenceFile...");
		for(int i = 0; i < lines; i++) {
			Text key = new Text("key" + i);
			Text value = new Text("value" + i);
			writer.append(key, value);
		}
		writer.close();

		// Read the data back and check
		LOG.info("Reading from the SequenceFile...");
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, filePath, conf);

		Writable key = (Writable) reader.getKeyClass().newInstance();
		Writable value = (Writable) reader.getValueClass().newInstance();

		int lc = 0;
		try {
			while(reader.next(key, value)) {
				Assert.assertEquals("key" + lc, key.toString());
				Assert.assertEquals("value" + lc, value.toString());
				lc++;
			}
		}
		finally {
			reader.close();
		}
		Assert.assertEquals(lines, lc);

		// Delete temporary files
		fs.delete(filePath, false);

		LOG.info("SUCCESS! Completed SequenceFileCodecTest with codec \"" + codecClass + "\"");
	}

	public static void main(String[] args) {
		int count = 10000;
		String codecClass = "org.apache.hadoop.io.compress.DefaultCodec";

		String usage = "TestCodec [-count N] [-codec <codec class>]";
		if(args.length == 0) {
			System.err.println(usage);
			System.exit(-1);
		}

		try {
			for(int i = 0; i < args.length; ++i) { // parse command line
				if(args[i] == null) {
					continue;
				}
				else if(args[i].equals("-count")) {
					count = Integer.parseInt(args[++i]);
				}
				else if(args[i].equals("-codec")) {
					codecClass = args[++i];
				}
			}

			Configuration conf = new Configuration();
			int seed = 0;
			codecTest(conf, seed, count, codecClass);
		}
		catch(Exception e) {
			System.err.println("Caught: " + e);
			e.printStackTrace();
		}

	}

}
