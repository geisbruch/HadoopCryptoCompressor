#Hadoop Crypto Compressor
It's a simple "compressor" for hadoop (really don't compress anythig) but enable you to
encrypt your data with public key "AES/CBC/PKCS5Padding" 

##How to use it?
It's simple, download the jar and put it in your hadoop classpath, so configure the compression codecs adding **org.apache.hadoop.io.compress.crypto.CyptoCodec**

Example:
    `<property>
      <name>io.compression.codecs</name>
      <value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,
        org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.crypto.CyptoCodec</value>
    </property>`

So if you use CryptoCodec as codec you will need to use as config property **cypto.secret.key** to put your public key

