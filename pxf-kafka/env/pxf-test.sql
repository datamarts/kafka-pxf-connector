create table source ( id int primary key, name varchar null);

insert into source values
  (101,'a'),(102,'b'),(103,'c'),
  (104,'d,e'),(105,'f,g,h'),(106,'i,j,k'),
  (107,null),(108,null),(109,null),
  (197,'x'),(198,'y'),(199,'z');

drop table source;

CREATE EXTENSION pxf;

-- fully manual way
CREATE WRITABLE EXTERNAL TABLE kafka_tbl ( id int, name varchar )
  LOCATION ('pxf://demo?kafka.bootstrap.servers=172.17.0.1:9094&ACCESSOR=org.greenplum.pxf.plugins.kafka.KafkaAccessor&RESOLVER=org.greenplum.pxf.plugins.kafka.KafkaResolver')
  FORMAT 'TEXT' (DELIMITER ',');

-- using profile (add kafka profile from env/pxf-profiles.xml to $PXF_CONF/conf/pxf-profiles.xml)
CREATE WRITABLE EXTERNAL TABLE kafka_tbl ( id int, name varchar )
    LOCATION ('pxf://demo?PROFILE=kafka&BOOTSTRAP_SERVERS=172.17.0.1:9094')
    FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

-- use env/kafka-site.xml to create server with name demosrv
CREATE WRITABLE EXTERNAL TABLE kafka_tbl ( id int, name varchar )
  LOCATION ('pxf://demo?PROFILE=kafka&SERVER=demosrv')
  FORMAT 'CUSTOM' (FORMATTER='pxfwritable_export');

drop external table kafka_tbl;

insert into kafka_tbl select * from source;
