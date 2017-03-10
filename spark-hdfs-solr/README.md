Utility to write big file from hdfs to solr using spark


##Steps to try out

###1. Build cmd
```
mvn clean package
```

###2. Sample run cmd
```
./spark-submit --class com.fs.spark.solr.writer.FreestoneSolrWriter  spark-hdfs-solr-1.0.0-jar-with-dependencies.jar --input_hdfs_path /user/hayat/test_data.json --format json --zkhost_str host:port/infra-solr --solr_collection test_collection --batch_size 10000 --log_file_path /var/log/solr_writer/solrwriter.log --application_name test-solr-writer --spark_master yarn-client --log_level DEBUG 

```
