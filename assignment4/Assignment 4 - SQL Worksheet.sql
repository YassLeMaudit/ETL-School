/*LBE
Code for Stream and Task Workshop - Student Version
Interactive learning with exercises and questions

INSTRUCTIONS FOR STUDENTS:
==========================

1. PROGRESSIVE DIFFICULTY:
   - Section 1-2: BEGINNER (Basic setup and data ingestion)
   - Section 3-4: INTERMEDIATE (JSON parsing and streams)
   - Section 5-6: ADVANCED (Task orchestration and monitoring)
   - Section 7-8: EXPERT (Complex orchestration and optimization)
   - Section 9-10: MASTER LVL 100 BOSS (Data quality and PII protection)

2. LEARNING APPROACH:
   - Start with Section 1 and work sequentially
   - Each section builds on the previous one
   - Complete all YOUR CODE HERE sections
   - Answer all questions for deeper understanding
   - Use hints when needed - they're there to help!

3. HINTS SYSTEM:
   - All hints and solutions are in a separate file: snowflake_streams_tasks_hints.sql
   - Try to solve exercises first without looking at hints
   - When stuck, check the hints file for the corresponding exercise number
   - Hints are numbered to match exercise numbers (e.g., HINT 1.1, HINT 1.2, etc.)

4. ASSESSMENT:
   - Complete all sections for basic understanding
   - Master sections 1-6 for intermediate level
   - Complete sections 7-10 for advanced level
   - Bonus challenges for expert level

Good luck with your Snowflake learning !
*/

-- ===========================================
-- SECTION 1: SETUP AND PREPARATION (BEGINNER LEVEL)
-- ===========================================

-- Exercise 1.1: Create the necessary role and permissions
-- DIFFICULTY: BEGINNER
-- TODO: Complete the role creation and grant statements below
-- HINTS: Check hints file for HINT 1.1 through HINT 1.5

use role ACCOUNTADMIN;
set myname = current_user();

create role if not exists Data_ENG; -- Create role Data_ENG
grant role Data_ENG to user identifier($myname); -- Grant role to current user
grant create database on account to role Data_ENG; -- Grant create database permission
grant execute task on account to role Data_ENG;  -- Grant task execution permissions
grant imported privileges on database snowflake to role Data_ENG; -- Grant imported privileges on SNOWFLAKE database

-- Exercise 1.2: Create warehouse and database
-- DIFFICULTY: BEGINNER
-- TODO: Create a warehouse and database for this lab
-- HINTS: Check hints file for HINT 1.6 through HINT 1.9

create warehouse if not exists Orchestration_WH
  warehouse_size = XSMALL
  auto_suspend = 300 -- 5 min
  auto_resume = true
  initially_suspended = true; -- Create warehouse Orchestration_WH (XSMALL, auto-suspend 5 min)
grant usage, operate on warehouse Orchestration_WH to role Data_ENG; -- Grant warehouse privileges to Data_ENG role
create database if not exists Credit_card; -- Create database Credit_card
grant usage, create schema, monitor on database Credit_card to role Data_ENG; -- Grant database privileges to Data_ENG role


-- Switch to the new role and database
use role Data_ENG;
use database Credit_card;
use schema PUBLIC;
use warehouse Orchestration_WH;

-- ===========================================
-- SECTION 2: DATA INGESTION SETUP (BEGINNER LEVEL)
-- ===========================================

-- Exercise 2.1: Create staging infrastructure
-- DIFFICULTY: BEGINNER
-- TODO: Create the necessary objects for data ingestion
-- HINTS: Check hints file for HINT 2.1 through HINT 2.2
use role ACCOUNTADMIN;  
grant usage on schema CREDIT_CARD.PUBLIC to role DATA_ENG;
grant create file format on schema CREDIT_CARD.PUBLIC to role DATA_ENG;

grant usage on warehouse ORCHESTRATION_WH to role DATA_ENG;


create or replace file format CC_JSON_FMT
  type = json; -- Create internal stage CC_STAGE with JSON file format
create or replace stage CC_STAGE
  file_format = CC_JSON_FMT; 
create or replace table CC_TRANS_STAGING (
  payload variant
);-- Create staging table CC_TRANS_STAGING with VARIANT column

-- Question 2.1: Why do we use a VARIANT column for JSON data?
-- Answer: We use a VARIANT column because it can store semi-structured data like JSON.

-- Exercise 2.2: Create the data generation stored procedure
-- TODO: This is provided for you - study the Java code to understand how it works

create or replace procedure SIMULATE_KAFKA_STREAM(mystage STRING,prefix STRING,numlines INTEGER)
  RETURNS STRING
  LANGUAGE JAVA
  PACKAGES = ('com.snowflake:snowpark:latest')
  HANDLER = 'StreamDemo.run'
  AS
  $$
    import com.snowflake.snowpark_java.Session;
    import java.io.*;
    import java.util.HashMap;
    public class StreamDemo {
      public String run(Session session, String mystage,String prefix,int numlines) {
        SampleData SD=new SampleData();
        BufferedWriter bw = null;
        File f=null;
        try {
            f = File.createTempFile(prefix, ".json");
            FileWriter fw = new FileWriter(f);
	        bw = new BufferedWriter(fw);
            boolean first=true;
            bw.write("[");
            for(int i=1;i<=numlines;i++){
                if (first) first = false;
                else {bw.write(",");bw.newLine();}
                bw.write(SD.getDataLine(i));
            }
            bw.write("]");
            bw.close();
            return session.file().put(f.getAbsolutePath(),mystage,options)[0].getStatus();
        }
        catch (Exception ex){
            return ex.getMessage();
        }
        finally {
            try{
	            if(bw!=null) bw.close();
                if(f!=null && f.exists()) f.delete();
	        }
            catch(Exception ex){
	            return ("Error in closing:  "+ex);
	        }
        }
      }

      private static final HashMap<String,String> options = new HashMap<String, String>() {
        { put("AUTO_COMPRESS", "TRUE"); }
      };

      public static class SampleData {
      private static final java.util.Random R=new java.util.Random();
      private static final java.text.NumberFormat NF_AMT = java.text.NumberFormat.getInstance();
      String[] transactionType={"PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","PURCHASE","REFUND"};
      String[] approved={"true","true","true","true","true","true","true","true","true","true","false"};
      static {
        NF_AMT.setMinimumFractionDigits(2);
        NF_AMT.setMaximumFractionDigits(2);
        NF_AMT.setGroupingUsed(false);
      }

      private static int randomQty(int low, int high){
        return R.nextInt(high-low) + low;
      }

      private static double randomAmount(int low, int high){
        return R.nextDouble()*(high-low) + low;
      }

      private String getDataLine(int rownum){
        StringBuilder sb = new StringBuilder()
            .append("{")
            .append("\"element\":"+rownum+",")
            .append("\"object\":\"basic-card\",")
            .append("\"transaction\":{")
            .append("\"id\":"+(1000000000 + R.nextInt(900000000))+",")
            .append("\"type\":"+"\""+transactionType[R.nextInt(transactionType.length)]+"\",")
            .append("\"amount\":"+NF_AMT.format(randomAmount(1,5000)) +",")
            .append("\"currency\":"+"\"USD\",")
            .append("\"timestamp\":\""+java.time.Instant.now()+"\",")
            .append("\"approved\":"+approved[R.nextInt(approved.length)]+"")
            .append("},")
            .append("\"card\":{")
                .append("\"number\":"+ java.lang.Math.abs(R.nextLong()) +"")
            .append("},")
            .append("\"merchant\":{")
            .append("\"id\":"+(100000000 + R.nextInt(90000000))+"")
            .append("}")
            .append("}");
        return sb.toString();
      }
    }
}
$$;

-- Question 2.2: What does this stored procedure simulate?
-- Answer: This stored procedure simulates a Kafka-like data stream by generating fake credit card transaction events in JSON format, saving them as a file, and uploading that file into a Snowflake stage.

-- Exercise 2.3: Test data generation
-- DIFFICULTY: BEGINNER
-- TODO: Call the stored procedure and verify the results
-- HINTS: Check hints file for HINT 2.3 through HINT 2.6

call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'cc_txn_', 1000); -- Call SIMULATE_KAFKA_STREAM with appropriate parameters
list @CC_STAGE; -- List files in the stage to verify creation
create or replace file format CC_JSON_FMT
  type = json
  strip_outer_array = true; -- Copy data from stage to staging table
copy into CC_TRANS_STAGING
  from @CC_STAGE
  file_format = (format_name = CC_JSON_FMT)
  on_error = 'ABORT_STATEMENT'; -- Check row count in staging table

-- ===========================================
-- SECTION 3: JSON DATA EXPLORATION (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 3.1: Explore JSON structure
-- DIFFICULTY: INTERMEDIATE
-- TODO: Write queries to explore the JSON data structure

select
  payload:card:number::string as card_number
from CC_TRANS_STAGING
limit 10; -- Select card numbers from the JSON data
select
  payload:transaction:id::number as txn_id,
  payload:transaction:amount::float as amount,
  payload:transaction:currency::string as currency,
  payload:transaction:approved::boolean as approved,
  payload:transaction:type::string as txn_type,
  to_timestamp_ntz(payload:transaction:timestamp::string) as ts
from CC_TRANS_STAGING
limit 10; -- Parse and display transaction details (id, amount, currency, approved, type, timestamp)

select
  payload:transaction:id::number as txn_id,
  payload:transaction:amount::float as amount,
  payload:transaction:type::string as txn_type,
  payload:transaction:approved::boolean as approved
from CC_TRANS_STAGING
where payload:transaction:amount::float < 600; -- Filter transactions with amount < 600

-- Question 3.1: What is the advantage of using VARIANT columns for JSON data?
-- Answer: The advantage of using a VARIANT column is that it can store JSON data in its original flexible structure, without needing a fixed schema.

-- Exercise 3.2: Create a normalized view
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a view that flattens the JSON structure into columns

create or replace view CC_TRANS_STAGING_VIEW as
select
  payload:element::number as element,
  payload:object::string as object_type,

  payload:transaction:id::number as txn_id,
  payload:transaction:type::string as txn_type,
  payload:transaction:amount::float as amount,
  payload:transaction:currency::string as currency,
  to_timestamp_ntz(payload:transaction:timestamp::string) as txn_ts,
  payload:transaction:approved::boolean as approved,

  payload:card:number::string as card_number,
  payload:merchant:id::number as merchant_id,

  payload as raw_payload
from CC_TRANS_STAGING; -- Create view CC_TRANS_STAGING_VIEW with proper column mapping
alter table CC_TRANS_STAGING set change_tracking = true;
alter view  CC_TRANS_STAGING_VIEW set change_tracking = true; -- Enable change tracking on table and view
select * from CC_TRANS_STAGING_VIEW limit 10;

select txn_id, amount, txn_type, approved
from CC_TRANS_STAGING_VIEW
where amount < 600
order by amount;

select txn_type, count(*) as n
from CC_TRANS_STAGING_VIEW
group by 1
order by 2 desc;

select txn_id, txn_ts, currency, amount, approved
from CC_TRANS_STAGING_VIEW
order by txn_ts desc
limit 10;-- Test the view with sample queries

-- Question 3.2: Why do we need to enable change tracking?
-- Answer: We enable change tracking so Snowflake can keep metadata about row changes in the background. This lets us query which rows were added, updated, or deleted without rescanning the entire table or view

-- ===========================================
-- SECTION 4: STREAMS AND CHANGE DATA CAPTURE (INTERMEDIATE LEVEL)
-- ===========================================

-- Exercise 4.1: Create and test streams
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a stream on the view and explore its behavior

create or replace stream CC_TRANS_STAGING_VIEW_STREAM
  on view CC_TRANS_STAGING_VIEW
  show_initial_rows = true; -- Create stream CC_TRANS_STAGING_VIEW_STREAM on the view
show streams like 'CC_TRANS_STAGING_VIEW_STREAM';

select * from CC_TRANS_STAGING_VIEW_STREAM; -- Check initial stream content
select count(*) as stream_row_count
from CC_TRANS_STAGING_VIEW_STREAM;-- Count records in the stream

-- Question 4.1: What does SHOW_INITIAL_ROWS=true do in stream creation?
-- Answer: When you create a stream with SHOW_INITIAL_ROWS = TRUE, Snowflake will include all of the existing rows in the source table or view as if they were newly inserted changes.

-- Exercise 4.2: Create analytical table
-- DIFFICULTY: INTERMEDIATE
-- TODO: Create a normalized table for analytics

create or replace table CC_TRANS_ALL (
  element       number,
  object_type   string,
  txn_id        number,
  txn_type      string,
  amount        float,
  currency      string,
  txn_ts        timestamp_ntz,
  approved      boolean,
  card_number   string,
  merchant_id   number,
  load_ts       timestamp_ntz default current_timestamp()
); -- Create table CC_TRANS_ALL with proper schema

insert into CC_TRANS_ALL (
  element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
)
select
  element,
  object_type,
  txn_id,
  txn_type,
  amount,
  currency,
  txn_ts,
  approved,
  card_number,
  merchant_id
from CC_TRANS_STAGING_VIEW_STREAM
where METADATA$ACTION = 'INSERT'; -- Insert data from stream into analytical table

select count(*) as row_count from CC_TRANS_ALL;
select txn_id, txn_ts, amount, currency, approved
from CC_TRANS_ALL
order by txn_ts desc
limit 10; -- Verify data in analytical table

-- Question 4.2: What is the difference between the staging table and analytical table?
-- Answer: The staging table stores raw JSON in a VARIANT column, while the analytical table stores structured, typed columns for easy querying and analysis.

-- ===========================================
-- SECTION 5: TASK ORCHESTRATION (ADVANCED LEVEL)
-- ===========================================

-- Exercise 5.1: Create your first task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that generates data automatically

create or replace task GENERATE_TASK
  warehouse = Orchestration_WH
  schedule = '1 minute'
as
  call SIMULATE_KAFKA_STREAM('@CC_STAGE', 'cc_txn_', 100); -- Create task GENERATE_TASK with 1-minute schedule
describe task GENERATE_TASK; -- Describe the task to see its definition
execute task GENERATE_TASK; -- Execute the task manually
alter task GENERATE_TASK resume; -- Resume the task to run on schedule

-- Question 5.1: What are the benefits of using tasks vs manual execution?
-- Answer: Tasks run automatically on a schedule reduce human error, save time, and make pipelines reliable and repeatable compared to running statements manually.

-- Exercise 5.2: Create data processing task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes files from stage to staging table

create or replace task PROCESS_FILES_TASK
  warehouse = Orchestration_WH
  schedule = '3 minute'
as
  copy into CC_TRANS_STAGING
    from @CC_STAGE
    file_format = (format_name = CC_JSON_FMT)
    pattern = '.*cc_txn_.*\\.json(\\.gz)?$' -- Filter only files whose names start with cc_txn_, end with .json, and optionally .json.gz if compressed.
    on_error = 'ABORT_STATEMENT';-- Create task PROCESS_FILES_TASK with 3-minute schedule

execute task PROCESS_FILES_TASK;
select count(*) as row_count from CC_TRANS_STAGING;
select *
from table(
  information_schema.copy_history(
    table_name => 'CC_TRANS_STAGING',
    start_time => dateadd('hour', -1, current_timestamp())
  )
); -- Execute task manually and verify results
alter task PROCESS_FILES_TASK resume; -- Resume the task

-- Exercise 5.3: Create data refinement task
-- DIFFICULTY: ADVANCED
-- TODO: Create a task that processes stream data into analytical table

alter task PROCESS_FILES_TASK suspend;
    
create or replace task REFINE_TASK
  warehouse = Orchestration_WH
  after PROCESS_FILES_TASK
  when system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM')
as
  insert into CC_TRANS_ALL (
    element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  )
  select
    element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  from CC_TRANS_STAGING_VIEW_STREAM
  where METADATA$ACTION = 'INSERT'; -- Create task REFINE_TASK with stream condition

alter task PROCESS_FILES_TASK resume;
execute task CREDIT_CARD.PUBLIC.PROCESS_FILES_TASK;  -- Triggers REFINE_TASK if the condition is true.
select count(*) as rows_in_all from CC_TRANS_ALL;
select * from CC_TRANS_ALL order by load_ts desc limit 10; -- Execute task manually and verify results

-- (optional) Check recent task runs
select *
from table(information_schema.task_history(
  scheduled_time_range_start => dateadd('hour', -1, current_timestamp())
))
where name in ('PROCESS_FILES_TASK','REFINE_TASK')
order by scheduled_time desc;


-- Question 5.3: What does SYSTEM$STREAM_HAS_DATA() do?
-- Answer: It’s used in tasks to make them run only when new data is available.

-- ===========================================
-- SECTION 6: MONITORING AND REPORTING (ADVANCED LEVEL)
-- ===========================================

-- Exercise 6.1: Monitor task execution
-- DIFFICULTY: ADVANCED
-- TODO: Create monitoring queries

select *
from table(information_schema.copy_history(
  table_name => 'CREDIT_CARD.PUBLIC.CC_TRANS_STAGING',
  start_time => dateadd(hour, -24, current_timestamp())
))
order by last_load_time desc; -- Query load history from INFORMATION_SCHEMA

select *
from table(information_schema.copy_history(
  table_name => 'CREDIT_CARD.PUBLIC.CC_TRANS_STAGING',
  start_time => dateadd(hour, -1, current_timestamp())
)); -- Query load history from ACCOUNT_USAGE

-- INFORMATION_SCHEMA (table function)
select *
from table(information_schema.task_history(
  scheduled_time_range_start => dateadd(hour, -24, current_timestamp())
))
where name in ('PROCESS_FILES_TASK','REFINE_TASK','GENERATE_TASK')
order by scheduled_time desc;

-- ACCOUNT_USAGE (vue)
select *
from snowflake.account_usage.task_history
where name in ('PROCESS_FILES_TASK','REFINE_TASK','GENERATE_TASK')
  and scheduled_time >= dateadd(hour, -24, current_timestamp())
order by scheduled_time desc; -- Check task execution history

-- Question 6.1: What is the difference between INFORMATION_SCHEMA and ACCOUNT_USAGE?
-- Answer: INFORMATION_SCHEMA views show metadata and history in real time, but only for the current database and usually with a shorter retention window. ACCOUNT_USAGE views cover the whole account with longer history (up to a year) and more detail, but data is delayed by ~1–3 hours.

-- Exercise 6.2: Analyze data flow
-- DIFFICULTY: ADVANCED
-- TODO: Create queries to understand data flow

select 'CC_TRANS_STAGING' obj, count(*) n from CC_TRANS_STAGING
union all
select 'CC_TRANS_STAGING_VIEW', count(*) from CC_TRANS_STAGING_VIEW
union all
select 'CC_TRANS_STAGING_VIEW_STREAM', count(*) from CC_TRANS_STAGING_VIEW_STREAM
union all
select 'CC_TRANS_ALL', count(*) from CC_TRANS_ALL; -- Count records in each table (staging, view, stream, analytical)
select max(txn_ts) as latest_txn_ts
from CC_TRANS_ALL; -- Find the latest transaction timestamp
select approved, count(*) as n, round(100.0*count(*)/sum(count(*)) over (),2) as pct
from CC_TRANS_ALL
group by approved
order by n desc; -- Analyze transaction patterns (approved vs rejected)

select txn_type, approved, count(*) as n
from CC_TRANS_ALL
group by txn_type, approved
order by txn_type, approved desc;
-- ===========================================
-- SECTION 7: ADVANCED ORCHESTRATION (EXPERT LEVEL)
-- ===========================================

-- Exercise 7.1: Create task dependencies
-- DIFFICULTY: EXPERT
-- TODO: Create a sequential task pipeline

create or replace task PIPE2_ROOT
  warehouse = Orchestration_WH
  schedule = '1 minute'
as
  select 1;  

create or replace file format CC_JSON_FMT type = json strip_outer_array = true;

create or replace task PIPE2_GENERATE
  warehouse = Orchestration_WH
as
  call SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_txn_',100);

create or replace task PIPE2_LOAD
  warehouse = Orchestration_WH
as
  copy into CC_TRANS_STAGING
  from @CC_STAGE
  file_format = (format_name = CC_JSON_FMT)
  pattern = '.*cc_txn_.*\\.json(\\.gz)?$'
  on_error = 'ABORT_STATEMENT';

create or replace task PIPE2_REFINE
  warehouse = Orchestration_WH
  when system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM')
as
  insert into CC_TRANS_ALL (
    element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  )
  select element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  from CC_TRANS_STAGING_VIEW_STREAM
  where METADATA$ACTION = 'INSERT'; -- Create tasks for pipeline 2

alter task PIPE2_GENERATE add after PIPE2_ROOT;
alter task PIPE2_LOAD add after PIPE2_GENERATE;
alter task PIPE2_REFINE add after PIPE2_LOAD; -- Set up task dependencies using ALTER TASK ... ADD AFTER

select system$task_dependents_enable('CREDIT_CARD.PUBLIC.PIPE2_ROOT');-- Create a root task that triggers the pipeline

execute task CREDIT_CARD.PUBLIC.PIPE2_ROOT; -- manually execution

select *
from table(information_schema.task_history(scheduled_time_range_start => dateadd(hour,-1,current_timestamp())))
where name in ('PIPE2_ROOT','PIPE2_GENERATE','PIPE2_LOAD','PIPE2_REFINE')
order by scheduled_time desc;  -- Check-up

-- Question 7.1: How do task dependencies work in Snowflake?
-- Answer: In Snowflake, task dependencies let child tasks run after their parent tasks finish, forming pipelines where tasks execute in order.

-- Exercise 7.2: Parallel processing
-- DIFFICULTY: EXPERT
-- TODO: Create tasks that can run in parallel

create or replace task PIPE3_ROOT
  warehouse = Orchestration_WH
  schedule = '1 minute'
as select 1;

create or replace task PIPE3_GEN_A
  warehouse = Orchestration_WH
as
begin
  call SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_txn_a_',100);
  copy into CC_TRANS_STAGING
    from @CC_STAGE
    file_format = (format_name = CC_JSON_FMT)
    pattern = '.*cc_txn_a_.*\\.json(\\.gz)?$'
    on_error = 'ABORT_STATEMENT';
end;

create or replace task PIPE3_GEN_B
  warehouse = Orchestration_WH
as
begin
  call SIMULATE_KAFKA_STREAM('@CC_STAGE','cc_txn_b_',100);
  copy into CC_TRANS_STAGING
    from @CC_STAGE
    file_format = (format_name = CC_JSON_FMT)
    pattern = '.*cc_txn_b_.*\\.json(\\.gz)?$'
    on_error = 'ABORT_STATEMENT';
end; -- Create a wait task for parallel processing

-- Create the task without multiple AFTERs
create or replace task PIPE3_WAIT_ALL
  warehouse = Orchestration_WH
  when system$stream_has_data('CC_TRANS_STAGING_VIEW_STREAM')
as
  insert into CC_TRANS_ALL (
    element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  )
  select element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id
  from CC_TRANS_STAGING_VIEW_STREAM
  where METADATA$ACTION = 'INSERT';

-- Now add both parents
alter task PIPE3_WAIT_ALL add after PIPE3_GEN_A;
alter task PIPE3_WAIT_ALL add after PIPE3_GEN_B;


select system$task_dependents_enable('CREDIT_CARD.PUBLIC.PIPE3_ROOT'); -- Set up parallel task execution
show tasks like 'PIPE3_%';


select * 
from table(information_schema.task_dependents(task_name => 'CREDIT_CARD.PUBLIC.PIPE3_ROOT'));

select *
from table(information_schema.task_history(
  scheduled_time_range_start => dateadd(hour,-24,current_timestamp())
))
where name in ('PIPE3_ROOT','PIPE3_GEN_A','PIPE3_GEN_B','PIPE3_WAIT_ALL')
order by scheduled_time desc; -- Monitor task dependencies

-- ===========================================
-- SECTION 8: CLEANUP AND BEST PRACTICES (EXPERT LEVEL)
-- ===========================================

-- Exercise 8.1: Task management
-- DIFFICULTY: EXPERT
-- TODO: Properly manage task lifecycle

alter task if exists GENERATE_TASK suspend;
alter task if exists PROCESS_FILES_TASK suspend;
alter task if exists REFINE_TASK suspend;

alter task if exists PIPE2_ROOT suspend;
alter task if exists PIPE2_GENERATE suspend;
alter task if exists PIPE2_LOAD suspend;
alter task if exists PIPE2_REFINE suspend;

alter task if exists PIPE3_ROOT suspend;
alter task if exists PIPE3_GEN_A suspend;
alter task if exists PIPE3_GEN_B suspend;
alter task if exists PIPE3_WAIT_ALL suspend;-- Suspend all running tasks

show tasks in schema CREDIT_CARD.PUBLIC;
select  "name", "state"
from table(result_scan(last_query_id()));; -- Show all tasks and their states


-- 1) Suspend task
alter task if exists GENERATE_TASK suspend;
alter task if exists PROCESS_FILES_TASK suspend;
alter task if exists REFINE_TASK suspend;
alter task if exists PIPE2_ROOT suspend;
alter task if exists PIPE2_GENERATE suspend;
alter task if exists PIPE2_LOAD suspend;
alter task if exists PIPE2_REFINE suspend;
alter task if exists PIPE3_ROOT suspend;
alter task if exists PIPE3_GEN_A suspend;
alter task if exists PIPE3_GEN_B suspend;
alter task if exists PIPE3_WAIT_ALL suspend;

-- 2) drop tasks
drop task if exists GENERATE_TASK;
drop task if exists PROCESS_FILES_TASK;
drop task if exists REFINE_TASK;
drop task if exists PIPE2_ROOT;
drop task if exists PIPE2_GENERATE;
drop task if exists PIPE2_LOAD;
drop task if exists PIPE2_REFINE;
drop task if exists PIPE3_ROOT;
drop task if exists PIPE3_GEN_A;
drop task if exists PIPE3_GEN_B;
drop task if exists PIPE3_WAIT_ALL;

-- 3) Drop stream / view
drop stream if exists CC_TRANS_STAGING_VIEW_STREAM;
drop view   if exists CC_TRANS_STAGING_VIEW;

-- 4) Drop tables
drop table if exists CC_TRANS_ALL;
drop table if exists CC_TRANS_STAGING;

-- 5) Drop stage / file format / procédure
drop stage       if exists CC_STAGE;
drop file format if exists CC_JSON_FMT;
drop procedure   if exists SIMULATE_KAFKA_STREAM(string,string,integer); -- Create a cleanup script

-- Question 8.1: Why is it important to suspend tasks when not needed?
-- Answer: Suspending tasks avoids unnecessary executions, which saves compute credits and prevents unwanted data changes.

-- Exercise 8.2: Performance analysis
-- DIFFICULTY: EXPERT
-- TODO: Analyze the performance of your pipeline

with th as (
  select name, scheduled_time, completed_time
  from table(information_schema.task_history(
    scheduled_time_range_start => dateadd(hour, -24, current_timestamp())
  ))
  where database_name = 'CREDIT_CARD'
    and schema_name   = 'PUBLIC'
)
select
  min(scheduled_time) as pipeline_start,
  max(completed_time) as pipeline_end,
  datediff('second', min(scheduled_time), max(completed_time)) as total_seconds
from th; -- Calculate total processing time

select
  sum(row_count)  as rows_loaded,
  sum(file_size)  as bytes_loaded
from table(information_schema.copy_history(
  table_name => 'CREDIT_CARD.PUBLIC.CC_TRANS_STAGING',
  start_time => dateadd(hour,-24,current_timestamp())
));
 -- Analyze data volume processed
with th as (
  select name, scheduled_time, completed_time
  from table(information_schema.task_history(
    scheduled_time_range_start => dateadd(hour,-24,current_timestamp())
  ))
  where database_name = 'CREDIT_CARD'
    and schema_name   = 'PUBLIC'
)
select
  name,
  avg(datediff('second', scheduled_time, completed_time)) as avg_sec,
  max(datediff('second', scheduled_time, completed_time)) as p95ish_sec
from th
group by name
order by avg_sec desc; -- Identify potential bottlenecks

-- ===========================================
-- SECTION 9: COMPREHENSIVE DATA QUALITY CHECKS (MASTER LEVEL)
-- ===========================================

-- Exercise 9.1: Basic data quality validation
-- DIFFICULTY: INTERMEDIATE
-- TODO: Implement comprehensive data quality checks

select txn_id, count(*) n
from CC_TRANS_ALL
group by txn_id
having count(*) > 1
order by n desc; -- Check for duplicate transactions

select *
from CC_TRANS_ALL
where amount is null or amount < 0 or amount > 50000
   or txn_ts > current_timestamp(); -- Validate data types and ranges

select
  sum(case when txn_id      is null then 1 else 0 end) as null_txn_id,
  sum(case when amount      is null then 1 else 0 end) as null_amount,
  sum(case when currency    is null then 1 else 0 end) as null_currency,
  sum(case when txn_ts      is null then 1 else 0 end) as null_txn_ts,
  sum(case when approved    is null then 1 else 0 end) as null_approved,
  sum(case when card_number is null then 1 else 0 end) as null_card
from CC_TRANS_ALL; -- Check for missing values

select *
from CC_TRANS_ALL
where not regexp_like(card_number, '^[0-9]{13,19}$'); -- Validate card number format

with stats as (
  select avg(amount) m, stddev(amount) sd from CC_TRANS_ALL where amount is not null
)
select a.*
from CC_TRANS_ALL a, stats s
where a.amount is not null
  and s.sd is not null
  and (a.amount > s.m + 3*s.sd or a.amount < s.m - 3*s.sd); -- Check for data anomalies

-- Exercise 9.2: Advanced data quality metrics
-- DIFFICULTY: ADVANCED
-- TODO: Create comprehensive data quality dashboard

use database CREDIT_CARD; use schema PUBLIC;

create or replace table CC_DQ_METRICS (
  metric_group   string,
  metric_name    string,
  metric_value   number(38,6),
  metric_den     number(38,6),
  details        variant,
  computed_at    timestamp_ntz default current_timestamp()
); -- Create data quality metrics table

insert into CC_DQ_METRICS
with tot as (select count(*) as n from CC_TRANS_ALL)
select *
from (
  select 'COMPLETENESS','non_null_txn_id',      count(txn_id)/nullif(max(t.n),0),      max(t.n), null, current_timestamp() from CC_TRANS_ALL, tot t union all
  select 'COMPLETENESS','non_null_amount',      count(amount)/nullif(max(t.n),0),      max(t.n), null, current_timestamp() from CC_TRANS_ALL, tot t union all
  select 'COMPLETENESS','non_null_currency',    count(currency)/nullif(max(t.n),0),    max(t.n), null, current_timestamp() from CC_TRANS_ALL, tot t union all
  select 'COMPLETENESS','non_null_txn_ts',      count(txn_ts)/nullif(max(t.n),0),      max(t.n), null, current_timestamp() from CC_TRANS_ALL, tot t union all
  select 'COMPLETENESS','non_null_card_number', count(card_number)/nullif(max(t.n),0), max(t.n), null, current_timestamp() from CC_TRANS_ALL, tot t
); -- Calculate completeness metrics

insert into CC_DQ_METRICS
select metric_group, metric_name, metric_value, total, null, current_timestamp()
from (
  select 'VALIDITY' as metric_group,
         count(*)::number as total,
         avg(iff(regexp_like(card_number,'^[0-9]{13,19}$'),1,0))   as card_format_ok,
         avg(iff(amount between 1 and 50000,1,0))                 as amount_1_50000,
         avg(iff(currency in ('USD','EUR','GBP'),1,0))            as currency_allowed
  from CC_TRANS_ALL
)
unpivot(metric_value for metric_name in (card_format_ok, amount_1_50000, currency_allowed));-- Calculate validity metrics
-- Consistency metrics (types aligned)

insert into CC_DQ_METRICS
select metric_group, metric_name, metric_value, total, null, current_timestamp()
from (
  select
    'CONSISTENCY' as metric_group,
    count(*)::number                                 as total,
    cast(count(distinct txn_id)/nullif(count(*),0)        as number(38,6)) as txn_id_unique,
    cast(avg(iff(txn_ts <= current_timestamp(),1,0))      as number(38,6)) as ts_not_future,
    cast(avg(iff(txn_type <> 'REFUND' or amount > 0,1,0)) as number(38,6)) as refund_amount_positive
  from CC_TRANS_ALL
)
unpivot(metric_value for metric_name in (
  txn_id_unique, ts_not_future, refund_amount_positive
)); -- Calculate consistency metrics
create or replace view CC_DQ_DASHBOARD as
select metric_group, metric_name, metric_value, metric_den,
       round(metric_value*100,2) as pct, computed_at
from (
  select m.*,
         row_number() over (partition by metric_group, metric_name order by computed_at desc) as rn
  from CC_DQ_METRICS m
)
where rn = 1
order by metric_group, metric_name;

-- Check dashboard
select * from CC_DQ_DASHBOARD; -- Create data quality dashboard

-- Exercise 9.3: Data quality monitoring and alerting
-- DIFFICULTY: EXPERT
-- TODO: Implement automated data quality monitoring

create or replace procedure RUN_DQ()
returns string
language sql
as
$$
insert into CC_DQ_METRICS
select metric_group, metric_name, metric_value, total, null, current_timestamp()
from (
  select 'SIMPLE_CHECKS' as metric_group,
         count(*)::number as total,
         count(txn_id)/nullif(count(*),0) as non_null_txn_id,
         count(amount)/nullif(count(*),0) as non_null_amount,
         avg(iff(amount between 1 and 50000,1,0)) as amount_ok,
         avg(iff(currency in ('USD','EUR','GBP'),1,0)) as currency_ok
  from CC_TRANS_ALL
)
unpivot(metric_value for metric_name in (
  non_null_txn_id, non_null_amount, amount_ok, currency_ok
));

select 'OK';
$$; -- Create data quality monitoring procedure

create or replace task DQ_TASK
  warehouse = ORCHESTRATION_WH
  schedule = '10 minute'
as
  call RUN_DQ();

alter task DQ_TASK resume; -- Create automated quality check task

use database CREDIT_CARD; use schema PUBLIC;
create or replace table CC_DQ_ALERTS (
  alert_time   timestamp_ntz,
  metric_name  string,
  metric_value number(38,6),
  threshold    number(38,6),
  note         string
);
create or replace task DQ_SIMPLE_TASK
  warehouse = ORCHESTRATION_WH
  schedule  = '30 minute'
as
begin
  insert into CC_DQ_METRICS
  select metric_group, metric_name, metric_value, total, null, current_timestamp()
  from (
    select 'SIMPLE_CHECKS' as metric_group,
           count(*)::number as total,
           count(txn_id)/nullif(count(*),0) as non_null_txn_id,
           count(amount)/nullif(count(*),0) as non_null_amount,
           avg(iff(amount between 1 and 50000,1,0)) as amount_ok,
           avg(iff(currency in ('USD','EUR','GBP'),1,0)) as currency_ok
    from CC_TRANS_ALL
  )
  unpivot(metric_value for metric_name in (
    non_null_txn_id, non_null_amount, amount_ok, currency_ok
  ));

  insert into CC_DQ_ALERTS (alert_time, metric_name, metric_value, threshold, note)
  with last as (
    select metric_name, metric_value
    from CC_DQ_METRICS
    where metric_group = 'SIMPLE_CHECKS'
    qualify row_number() over (partition by metric_name order by computed_at desc) = 1
  ),
  th as (
    select * from values
      ('non_null_txn_id', 1.00),
      ('non_null_amount', 1.00),
      ('amount_ok',       0.95),
      ('currency_ok',     0.99)
    as t(metric_name, threshold)
  )
  select current_timestamp(), l.metric_name, l.metric_value, th.threshold, 'DQ threshold breached'
  from last l join th on l.metric_name = th.metric_name
  where l.metric_value < th.threshold;
end;

alter task DQ_SIMPLE_TASK resume;-- Implement quality alerting system


create or replace view DQ_TRENDS as
select metric_name,
       date_trunc('hour', computed_at) as hour,
       avg(metric_value) as avg_val
from CC_DQ_METRICS
group by metric_name, date_trunc('hour', computed_at)
order by hour desc; -- Create quality trend analysis

-- ===========================================
-- SECTION 10: PII PROTECTION AND DATA MASKING (MASTER LEVEL)
-- ===========================================

-- Exercise 10.1: Identify PII in credit card data
-- DIFFICULTY: INTERMEDIATE
-- TODO: Identify and categorize PII fields

select 
  count(*) as total_rows,
  sum(case when regexp_like(card_number,'^[0-9]{13,19}$') then 1 else 0 end) as pii_card_count
from CC_TRANS_ALL;

select 
  card_number,
  left(card_number,4) || '********' || right(card_number,4) as masked_preview
from CC_TRANS_ALL
limit 5;; -- Identify PII fields in the data
create or replace table CC_PII_CLASSIFICATION (
  object_name       string,
  column_name       string,
  pii_type          string,         
  sensitivity_level string,         
  masking_required  boolean,
  masking_rule      string,         
  rationale         string,
  classified_at     timestamp_ntz default current_timestamp()
); -- Create PII classification table
insert into CC_PII_CLASSIFICATION (object_name, column_name, pii_type, sensitivity_level, masking_required, masking_rule, rationale)
select * from values
  ('CC_TRANS_ALL','CARD_NUMBER','PAN','HIGH', true ,'MASK_LAST4','Primary Account Number (credit card)'),
  ('CC_TRANS_ALL','TXN_ID','Non-PII','LOW', false,'NONE','Technical transaction ID (not a person)'),
  ('CC_TRANS_ALL','AMOUNT','Non-PII','LOW', false,'NONE','Transaction amount'),
  ('CC_TRANS_ALL','CURRENCY','Non-PII','LOW', false,'NONE','Currency code'),
  ('CC_TRANS_ALL','TXN_TS','Quasi-identifier','MEDIUM', false,'NONE','Timestamp; may be sensitive depending on context'),
  ('CC_TRANS_ALL','APPROVED','Non-PII','LOW', false,'NONE','Binary status'),
  ('CC_TRANS_ALL','MERCHANT_ID','Non-PII','LOW', false,'NONE','Merchant ID (non-person entity)'),
  ('CC_TRANS_ALL','ELEMENT','Non-PII','LOW', false,'NONE','Technical field'),
  ('CC_TRANS_ALL','OBJECT_TYPE','Non-PII','LOW', false,'NONE','Object type'); -- Classify all fields by sensitivity and Determine masking requirements
 

-- Exercise 10.2: Implement data masking for PII
-- DIFFICULTY: ADVANCED
-- TODO: Create masked views for different user roles

use role ACCOUNTADMIN;
use database CREDIT_CARD; use schema PUBLIC;

create or replace view CC_TRANS_ALL_ANALYST_V as
select
  txn_id, txn_type, amount, currency, txn_ts, approved, merchant_id,
  regexp_replace(card_number, '^(\\d{0,15})(\\d{4})$', '***************\\2') as card_number_masked
from CC_TRANS_ALL; -- Create masked view for analysts
create or replace view CC_TRANS_ALL_AUDITOR_V as
select
  txn_id, txn_type, amount, currency, txn_ts, approved, merchant_id,
  sha2(card_number) as card_number_hash,
  regexp_replace(card_number, '^(\\d{0,15})(\\d{4})$', '***************\\2') as card_number_last4
from CC_TRANS_ALL;-- Create masked view for auditors

create role if not exists ANALYST_R;
create role if not exists AUDITOR_R;

grant usage on database CREDIT_CARD to role ANALYST_R;
grant usage on schema CREDIT_CARD.PUBLIC to role ANALYST_R;
grant select on view  CREDIT_CARD.PUBLIC.CC_TRANS_ALL_ANALYST_V to role ANALYST_R;

grant usage on database CREDIT_CARD to role AUDITOR_R;
grant usage on schema CREDIT_CARD.PUBLIC to role AUDITOR_R;
grant select on view  CREDIT_CARD.PUBLIC.CC_TRANS_ALL_AUDITOR_V to role AUDITOR_R; -- Implement role-based access control

-- Analyst test (should only see the last 4 digits)
grant role ANALYST_R to user ILYESKABBOUR;
use role ANALYST_R;
select * from CREDIT_CARD.PUBLIC.CC_TRANS_ALL_ANALYST_V limit 5;

-- Auditor test (must see hash + last 4 digits, never the full PAN)
USE ROLE ACCOUNTADMIN;
grant role AUDITOR_R to user ILYESKABBOUR;
use role AUDITOR_R;
select * from CREDIT_CARD.PUBLIC.CC_TRANS_ALL_AUDITOR_V limit 5; -- Test masking effectiveness

-- Exercise 10.3: Advanced PII protection strategies
-- DIFFICULTY: EXPERT
-- TODO: Implement advanced PII protection mechanisms

USE ROLE ACCOUNTADMIN;
create or replace masking policy PAN_MASK as (val string) returns string ->
  case
    when is_role_in_session('ACCOUNTADMIN') then val  -- admin: Full acces
    when is_role_in_session('AUDITOR_R')    then substr(sha2(val),1,12)||'...'||right(val,4) 
    when is_role_in_session('ANALYST_R')    then regexp_replace(val,'^(\\d{0,15})(\\d{4})$','***************\\2')
    else 'MASKED' 
  end; -- Create dynamic masking policy
alter table CC_TRANS_ALL modify column CARD_NUMBER set masking policy PAN_MASK; -- Apply masking to sensitive columns

alter table CC_TRANS_ALL set data_retention_time_in_days = 1;
create or replace task PII_RETENTION_TASK
  warehouse = ORCHESTRATION_WH
  schedule  = 'USING CRON 0 2 * * * Europe/Paris'  -- Daily
as
  delete from CC_TRANS_ALL
  where txn_ts < dateadd(day, -90, current_timestamp());
alter task PII_RETENTION_TASK resume; -- Implement data retention policy


create table if not exists CC_TRANS_ALL_ANON like CC_TRANS_ALL;
create or replace procedure PROC_ANONYMIZE()
  returns string
  language sql
  execute as owner
as
$$
begin
  truncate table CC_TRANS_ALL_ANON;

  insert into CC_TRANS_ALL_ANON (
    element, object_type, txn_id, txn_type, amount, currency, txn_ts, approved, card_number, merchant_id, load_ts
  )
  select
    element,
    object_type,
    txn_id,
    txn_type,
    amount,
    currency,
    txn_ts,
    approved,
    sha2(to_varchar(card_number)) as card_number,     
    abs(merchant_id) % 100  as merchant_id,     
    current_timestamp() as load_ts
  from CC_TRANS_ALL;

  return 'OK';
end;
$$;

-- Run and verify
call PROC_ANONYMIZE();
select * from CC_TRANS_ALL_ANON limit 5; -- Create data anonymization procedure

-- Test PII protection mechanisms
grant usage on database CREDIT_CARD to role ANALYST_R;
grant usage on schema CREDIT_CARD.PUBLIC to role ANALYST_R;
grant select on table CREDIT_CARD.PUBLIC.CC_TRANS_ALL to role ANALYST_R;
grant usage on database CREDIT_CARD to role AUDITOR_R;
grant usage on schema CREDIT_CARD.PUBLIC to role AUDITOR_R;
grant select on table CREDIT_CARD.PUBLIC.CC_TRANS_ALL to role AUDITOR_R;

set myname = current_user();
grant role ANALYST_R to user identifier($myname);
grant role AUDITOR_R to user identifier($myname);

use role ANALYST_R;
select card_number from CREDIT_CARD.PUBLIC.CC_TRANS_ALL limit 5;   

use role ACCOUNTADMIN;
use role AUDITOR_R;
select card_number from CREDIT_CARD.PUBLIC.CC_TRANS_ALL limit 5;   

use role ACCOUNTADMIN;
select card_number from CREDIT_CARD.PUBLIC.CC_TRANS_ALL limit 5;   

-- Question 10.1: What are the key principles of PII protection in data systems?
-- Answer: Protect PII by limiting access, masking sensitive fields and auditing usage.

-- Question 10.2: How would you implement GDPR compliance for this credit card data?
-- Answer: To ensure GDPR compliance, keep only the data you need, protect sensitive fields with masking or encryption, and control access with roles. Set retention limits and use tasks to delete or anonymize old data.

-- Question 10.3: What are the trade-offs between data utility and privacy protection?
-- Answer: Strong privacy (masking, anonymization) reduces risk but can limit data detail for analysis. More utility means less privacy, and more privacy means less detail.

