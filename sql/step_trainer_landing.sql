CREATE EXTERNAL TABLE `step_trainer_landing`(
  `sensorreadingtime` bigint COMMENT 'from deserializer', 
  `serialnumber` string COMMENT 'from deserializer', 
  `distancefromobject` int COMMENT 'from deserializer')
ROW FORMAT SERDE 
  'org.openx.data.jsonserde.JsonSerDe' 
WITH SERDEPROPERTIES ( 
  'paths'='distanceFromObject,sensorReadingTime,serialNumber') 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://step-trainer-s3-landing/step_trainer_landing/'
TBLPROPERTIES (
  'CRAWL_RUN_ID'='7d303fa7-3d2f-4125-8e4d-3db39f3ee3ba', 
  'CrawlerSchemaDeserializerVersion'='1.0', 
  'CrawlerSchemaSerializerVersion'='1.0', 
  'UPDATED_BY_CRAWLER'='step_trainer_landing_crawler', 
  'averageRecordSize'='1032', 
  'classification'='json', 
  'compressionType'='none', 
  'objectCount'='3', 
  'recordCount'='3194', 
  'sizeKey'='3298200', 
  'typeOfData'='file')
