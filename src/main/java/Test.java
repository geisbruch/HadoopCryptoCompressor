import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.InvalidParameterSpecException;
import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.crypto.CyptoCodec;

import sec.util.Crypto;

public class Test {


	public static void main(String[] args) throws NoSuchAlgorithmException, InvalidKeySpecException, NoSuchPaddingException, InvalidKeyException, InvalidParameterSpecException, IllegalBlockSizeException, BadPaddingException, IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		byte[] simpleStr = ("dsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsad" +
				"sadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhask" +
				"ljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasd" +
				"haskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhas" +
				"kljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskl" +
				"kljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskljdasdsadsadasdhaskl" +
				"dasdsadsadasdhaskljdasdsadsadasdhaskljj%%1").getBytes();
		CyptoCodec codec = new CyptoCodec();
		Configuration config = new Configuration();
		config.set(CyptoCodec.CRYPTO_SECRET_KEY,"Una clave cualquiera");
		codec.setConf(config);
		Crypto c = new Crypto("Una clave cualquiera");
		c.encrypt(simpleStr);
		CompressionOutputStream outStream = codec.createOutputStream(out);
		outStream.write(simpleStr);
		ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
		byte[] b = new byte[1];
		CompressionInputStream read = codec.createInputStream(in);
		
		while(read.read(b)!=-1){
			System.out.print(new String(b));
		}
	}

}
