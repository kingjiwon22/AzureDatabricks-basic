# Databricks notebook source
# MAGIC %md
# MAGIC # Demo 3

# COMMAND ----------

# MAGIC %md
# MAGIC ### Advanced Delta Lake Features  
# MAGIC 지금부터는 Databriks Delta Lake 테이블이 제공하는 고유한 기능 중 몇가지를 살펴보겠습니다.  

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 1. 테이블 생성 후 데이터 작업
# MAGIC 우선 데모를 위해서  테이블 생성과 데이터 적재 잡업을 진행하겠습니다.  
# MAGIC 제일 먼저 Students 테이블을 생성합니다.  
# MAGIC 그 후 한 건씩 데이터를 총 3번 넣어줍니다.  
# MAGIC 그리고 3건의 데이터를 한번에 넣어줍니다.  
# MAGIC 이름이 T로 시작하는 학생들의 value 값에 1을 더하여 update 해줍니다.  
# MAGIC 그 후 value 가 6보다 큰 값인 데이터는 삭제합니다.  
# MAGIC updates라는 임시 뷰를 생성해줍니다.  
# MAGIC 위에 만든 Students 테이블과 updates 임시 뷰를 merge하여 type이 update인 경우는 students테이블의 id를 update하고, delete인 경우에는 해당 id를 가진 데이터를 삭제하고, 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE students
# MAGIC   (id INT, name STRING, value DOUBLE);
# MAGIC   
# MAGIC INSERT INTO students VALUES (1, "Yve", 1.0);
# MAGIC INSERT INTO students VALUES (2, "Omar", 2.5);
# MAGIC INSERT INTO students VALUES (3, "Elia", 3.3);
# MAGIC 
# MAGIC INSERT INTO students
# MAGIC VALUES 
# MAGIC   (4, "Ted", 4.7),
# MAGIC   (5, "Tiffany", 5.5),
# MAGIC   (6, "Vini", 6.3);
# MAGIC   
# MAGIC UPDATE students 
# MAGIC SET value = value + 1
# MAGIC WHERE name LIKE "T%";
# MAGIC 
# MAGIC DELETE FROM students 
# MAGIC WHERE value > 6;
# MAGIC 
# MAGIC CREATE OR REPLACE TEMP VIEW updates(id, name, value, type) AS VALUES
# MAGIC   (2, "Omar", 15.2, "update"),
# MAGIC   (3, "", null, "delete"),
# MAGIC   (7, "Blue", 7.7, "insert"),
# MAGIC   (11, "Diya", 8.8, "update");
# MAGIC   
# MAGIC MERGE INTO students b
# MAGIC USING updates u
# MAGIC ON b.id=u.id
# MAGIC WHEN MATCHED AND u.type = "update"
# MAGIC   THEN UPDATE SET *
# MAGIC WHEN MATCHED AND u.type = "delete"
# MAGIC   THEN DELETE
# MAGIC WHEN NOT MATCHED AND u.type = "insert"
# MAGIC   THEN INSERT *;

# COMMAND ----------

# MAGIC %md
# MAGIC 생성한 테이블 students의 데이터를 확인해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from students

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 테이블 정보 살펴보기  
# MAGIC Stuents 테이블은 매우 작은 테이블이지만, 많은 수의 데이터 파일로 이루어져 있습니다.  
# MAGIC **`DESCRIBE EXTENDED`** , **`DESCRIBE DETAIL`** 구문 이용하여 테이블의 metadata를 살펴볼 수 있습니다.  

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 테이블의 기본 메타데이터 정보를 반환
# MAGIC DESCRIBE EXTENDED students

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 델타 테이블에 대한 파일 수(numFiles), 위치 등 자세한 세부정보 확인
# MAGIC DESCRIBE DETAIL students

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. Delta Lake 파일들 살펴보기  
# MAGIC 테이블 저장 경로 디렉토리에는 parquet 포맷의 데이터 파일들과 **`_delta_log`** 디렉토리가 있습니다.  
# MAGIC Delta Lake 테이블의 레코드들은 parquet 파일로 저장됩니다.  
# MAGIC 그리고 Delta Lake 테이블의 트랜잭션 기록들은 **`_delta_log`** 디렉토리 아래에 저장됩니다.

# COMMAND ----------

# MAGIC %md
# MAGIC Students 테이블 저장 경로 내 parquet 형식의 데이터 파일들과 **`_delta_log`** 폴더를 리스트로 확인해보겠습니다.

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/students"))

# COMMAND ----------

# MAGIC %md
# MAGIC delta_log 하위 파일들도 확인해보겠습니다.  
# MAGIC 각각의 트랜잭션 로그들은 이렇게 버전별 json 파일로 저장됩니다.  
# MAGIC 여기서는 총 8개의 트랜잭션 로그 파일을 볼 수 있습니다. (버전은 0부터 시작합니다.)

# COMMAND ----------

display(dbutils.fs.ls("dbfs:/user/hive/warehouse/students/_delta_log"))

# COMMAND ----------

# MAGIC %md 
# MAGIC #### 4. 과거 버전으로 Rollback 하기  
# MAGIC **`Time Travel`** 은 현재 버전에서 트랜잭션을 취소하거나 과거 상태의 데이터를 다시 생성하는 방식이 아니라, 트랜잭션 로그를 이용하여 해당 버전에서 유효한 파일들을 찾아낸 후 이들을 쿼리하는 방식으로 이루어집니다.  

# COMMAND ----------

# MAGIC %md
# MAGIC 우선 Students 테이블을 다시 확인합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from students

# COMMAND ----------

# MAGIC %md
# MAGIC 실수로 모든 레코드를 삭제한 상황을 가정해 보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM students

# COMMAND ----------

# MAGIC %md
# MAGIC 데이터가 모두 삭제된 Students 테이블을 조회합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from students

# COMMAND ----------

# MAGIC %md
# MAGIC 테이블 히스토리를 조회해보면 트랜잭션 기록을 확인할 수 있습니다.  
# MAGIC 삭제 커밋이 수행되기 이전 버전을 확인하기 위해 **`DESCRIBE HISTORY`** 구문을 실행해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history students

# COMMAND ----------

# MAGIC %md 
# MAGIC 이제 삭제 커밋이 수행되기 이전 버전으로 rollback 해봅니다. 

# COMMAND ----------

# MAGIC %sql
# MAGIC RESTORE TABLE students TO VERSION AS OF 7 

# COMMAND ----------

# MAGIC %md
# MAGIC rollback이 정상적으로 이루어졌는지 확인합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from students

# COMMAND ----------

# MAGIC %md 
# MAGIC 
# MAGIC 테이블 히스토리를 조회(DESCRIBE HISTORY)해 보면 <a href="https://docs.databricks.com/spark/latest/spark-sql/language-manual/delta-restore.html" target="_blank">RESTORE</a> 명령이 트랜잭션으로 기록됨을 볼 수 있습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history students

# COMMAND ----------


