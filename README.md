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
       org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.crypto.CryptoCodec</value>
    </property>

So if you use CryptoCodec as codec you will need to use as config property **crypto.secret.key** to put your private key 
(you can define it in runtime)

### From the command line
From the command line, you can encrypt and decrypt files using the following:
    java -classpath hadoopLib/ -jar target/HadoopCryptoCompressor-0.0.1-SNAPSHOT-jar-with-dependencies.jar -e -s "password" -b 4096 1-10001.sql.crypt 1-10001.sql.crypt

    java -classpath hadoopLib/ -jar target/HadoopCryptoCompressor-0.0.1-SNAPSHOT-jar-with-dependencies.jar -d -s "password" -b 4096 1-10001.sql.crypt 1-10001.sql.1

You can use the -h option to get more details.

### In other JVM applications (Groovy Below)
    //Decrypt file from S3 and consolidate in a single output stream
    FileEncryptDecrypter fed = new FileEncryptDecrypter(mySecretKey)
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream()
    list.objectSummaries.each {
        if(it.key.endsWith(".crypto")) {
            def obj = s3client.getObject(AUDIT_LOGS_BUCKET, it.key)
            fed.decryptFile(obj.objectContent,outputStream,1024)
        }
    }
