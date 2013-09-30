#Hadoop Crypto Compressor
It's a simple "compressor" for hadoop (really don't compress anythig) but enable you to
encrypt your data with public key "AES/CBC/PKCS5Padding" 

##How to use it?


### In Hadoop

It's simple, download the jar and put it in your hadoop classpath, so configure the compression codecs adding **org.apache.hadoop.io.compress.crypto.CryptoCodec**

Example:

    <property>
       <name>io.compression.codecs</name>
       <value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,
       org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.CryptoCodec</value>
    </property>

So if you use CryptoCodec as codec you will need to use as config property **crypto.secret.key** to put your private key 
(you can define it in runtime)

### From the command line
From the command line, you can encrypt and decrypt files using the following:

    java -jar target/HadoopCryptoCompressor-0.0.6-SNAPSHOT.jar -e -aeskey "key"  test test.crypto

    java -jar target/HadoopCryptoCompressor-0.0.6-SNAPSHOT.jar -d -aeskey "key"  test.crypto test.new

You can use the -h option to get more details.

