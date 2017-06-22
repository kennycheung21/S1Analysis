create table cases (Severity Int,  caseNum Int, status Int, DateOpen timestamp, DateClose timestamp, AccountID Int, AmbariVersionID Int, StackVersionID Int, componentID Int,  HBug Int, ABug Int, HotFix Int, EAR Int, RootCauseID Int, RMP Int, Env Int, ResolutionTime Int) PARTITIONED BY(year Int, week String) STORED AS ORC;

--In hive2
alter table cases ADD CONSTRAINT pk PRIMARY KEY (casenum) DISABLE NOVALIDATE;

--Key, value pair for each dimensions

create external table account_staging (name String, ID Int) ROW FORMAT DELIMITED  FIELDS TERMINATED BY '$' STORED AS TEXTFILE LOCATION '/tmp/accounts' tblproperties ("skip.header.line.count"="1");

Create table account (ID Int, name String) STORED AS ORC;

insert overwrite table account select cast(Id as Int), name from account_staging;

create external table product_staging (name String, ID Int) ROW FORMAT DELIMITED  FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION '/tmp/products' tblproperties ("skip.header.line.count"="1");

Create table product (name String, ID Int) STORED AS ORC;

insert overwrite table product select name, Id from product_staging;

Create table RootCause (name String, ID int) STORED AS ORC;

Create table AmbariVersion (Version String, ID Int) STORED AS ORC;

Create table StackVersion (Version String, ID Int) STORED AS ORC;

drop table if exists Automation;
Create table Automation (CaseNum Int, Case_Owner String, Description String, Actionable Boolean, JIRA String, status String)
clustered by(CaseNum) into 3 buckets
STORED AS ORC tblproperties ('transactional'='true');

--In hive2
ALTER TABLE Automation ADD CONSTRAINT fk_a FOREIGN KEY (CaseNum) REFERENCES cases(CaseNum) DISABLE NOVALIDATE RELY;

drop table if exists SmartSense;
Create table SmartSense (CaseNum Int, Existing_Rule String, Suggestion String, Suggestion_Owner String,  Reviewed_By String, JIRA String, status String)
partitioned by (Priority int)
clustered by(CaseNum) into 2 buckets
STORED AS ORC tblproperties ('transactional'='true');

--In hive2
ALTER TABLE SmartSense ADD CONSTRAINT fk_ss FOREIGN KEY (CaseNum) REFERENCES cases(CaseNum) DISABLE NOVALIDATE RELY;