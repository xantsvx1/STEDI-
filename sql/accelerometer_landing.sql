CREATE EXTERNAL TABLE `accelerometer_landing`(
  `user` string COMMENT 'from deserializer', 
  `timestamp` bigint COMMENT 'from deserializer', 
  `x` double COMMENT 'from deserializer', 
  `y` double COMMENT 'from deserializer', 
  `z` double COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='timestamp,user,x,y,z') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://accelerometer-s3-landing/accelerometer_landing/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='e0e17ec4-c515-473f-b5a0-eb31ab83c6ad', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='accelerometer_landing_crawler', 
  'averageRecordSize'='761', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='9', 
  'recordCount'='9007', 
  'sizeKey'='6871328', 
  'typeOfData'='file')
