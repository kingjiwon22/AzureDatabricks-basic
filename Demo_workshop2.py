# Databricks notebook source
# MAGIC %md
# MAGIC # Demo Scenario 2

# COMMAND ----------

# MAGIC %md
# MAGIC ### Python 사용하여 Datalake에 연결하기  

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. Storage 내 Container에 Mount  
# MAGIC Azure 서비스 주체와 함께 OAuth 2.0을 사용하여 Azure Databricks에서 Azure Data Lake Storage Gen2에 연결하는 데 필요한 모든 과정을 해보겠습니다.  
# MAGIC   
# MAGIC Tip : OAuth는 인터넷 사용자들이 비밀번호를 제공하지 않고 다른 웹사이트 상의 자신들의 정보에 대해 웹사이트나 애플리케이션의 접근 권한을 부여할 수 있는 공통적인 수단으로서 사용되는, 접근 위임을 위한 개방형 표준 프로토콜  
# MAGIC ex.외부 소셜 계정을 기반으로 간편히 회원가입 및 로그인 할 수 있는 기능에 사용되는 프로토콜이 OAuth 입니다. (카카오로 시작하기, 네이버로 시작하기 등)  
# MAGIC   
# MAGIC [[참고] 자습서: Azure Data Lake Storage Gen2에 연결(learn.microsoft)](https://learn.microsoft.com/ko-kr/azure/databricks/getting-started/connect-to-azure-storage)*

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "<application-id>", 
       "fs.azure.account.oauth2.client.secret": "<client-secret>",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<tenant-id>/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/",  # 소스 데이터가 위치한 경로
mount_point = "/mnt/<folder-name>",  # 소스를 mount할 DBFS내 디렉토리. 이때 꼭 /mnt 하위에 위치하도록 설정해야함.
extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC - application id : Azure Active Directory > 앱 등록 > 개요 > 애플리케이션(클라이언트)ID
# MAGIC - client secret : Azure Active Directory > 앱 등록 > 인증서 및 암호 > 클라이언트 암호의 [값]
# MAGIC - tenant id: Azure Active Directory > 앱 등록 > 개요 > 디렉터리(테넌트)ID

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
       "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
       "fs.azure.account.oauth2.client.id": "0245d618-fb68-450b-bdf7-2072ab0f9667",
       "fs.azure.account.oauth2.client.secret": "Z~Z8Q~f2eK-tJ5k.if_ULDO_a7wzUizy6StHsdgK",
       "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/785087ba-1e72-4e7d-b1d1-4a9639137a66/oauth2/token",
       "fs.azure.createRemoteFileSystemDuringInitialization": "true"}

dbutils.fs.mount(
source = "abfss://oauth@storageadbworkshop.dfs.core.windows.net/",
mount_point = "/mnt/using_oauth",
extra_configs = configs)

# COMMAND ----------

# MAGIC %md
# MAGIC  Databricks 유틸리티 **`dbutils`** 를 사용하면 강력한 작업 조합을 쉽게 수행할 수 있습니다.  
# MAGIC  유틸리티를 사용하여 개체 스토리지를 효율적으로 사용하고, Notebook을 연결 및 매개 변수화하고, Secret을 사용할 수 있습니다.  
# MAGIC  **`dbutils`** 는 Notebook 외부에서 지원되지 않습니다.  
# MAGIC  **`dbutils`** 는 python, R , Scala Notebook에서 사용할 수 있습니다.

# COMMAND ----------

#각 유틸리티에 대한 간단한 설명과 함께 사용 가능한 유틸리티를 나열하려면 Python 또는 Scala에 dbutils.help()를 실행
dbutils.help()

# COMMAND ----------

#DBFS(Databricks 파일 시스템) 유틸리티에 사용 가능한 명령을 나열합니다.
dbutils.fs.help()

# COMMAND ----------

#명령 중 자세한 예시 및 변수에 대한 정보를 보고 싶다면 help() 안에 해당 명령어를 입력해주고 실행합니다.
dbutils.fs.help("mount")

# COMMAND ----------

# MAGIC %md
# MAGIC 참고) 스토리지 계정의 엑세스 키를 통해서 바로 컨테이너에 mount하는 방법입니다.  
# MAGIC *이는 보안에 취약하므로 권장하지 않습니다.* 

# COMMAND ----------

dbutils.fs.mount(
   source='wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/',
   mount_point = '/mnt/fileupload',
   extra_configs = {'fs.azure.account.key.<storage-account-name>.blob.core.windows.net':'<storage-access-key>'} #그외 구성 값 : 스토리지 계정의 액세스 키
)

# COMMAND ----------

dbutils.fs.mount(
   source='wasbs://upload@storageadbworkshop.blob.core.windows.net/',
   mount_point = '/mnt/fileUpload',
   extra_configs = {'fs.azure.account.key.storageadbworkshop.blob.core.windows.net':'VV0KSKOelNlQ/ZWJzWCTsL67Zr2KKjE1d536BOD/kXqI1RIDlqege3/c5wWzxvNnNIxND7R3wDFH+AStYl+kIA=='}
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. mount한 경로 내 파일을 확인  
# MAGIC mount가 되면 해당 경로 내 파일을 확인해줍니다.

# COMMAND ----------

dbutils.fs.ls('/mnt/fileUpload')

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. 파일 정보를 Dataframe에 적재  
# MAGIC 이제 mount한 경로 내 파일 OrderLines.csv를 Dataframe에 담습니다. 

# COMMAND ----------

# Dataframe에 담기
df = spark.read.format('csv').options(header='true', inferschema='true').load('/mnt/fileUpload/OrderLines.csv')

# COMMAND ----------

# dataframe에 담긴 내용을 테이블 형식으로 확인
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. Dataframe을 테이블로 생성  
# MAGIC Dataframe에 데이터가 정상적으로 적재된 것을 확인하였습니다.  
# MAGIC 이제 이 Dataframe을 테이블로 생성해보겠습니다.  
# MAGIC format은 델타, mode는 덮어쓰기, 테이블명은 Orderlines로 설정 후 실행합니다.  
# MAGIC 참고) Azure Databricks의 모든 테이블은 기본적으로 **Delta Table**입니다.

# COMMAND ----------

# df를 테이블로 생성하기
df.write.format("delta").mode("overwrite").saveAsTable("OrderLines")

# COMMAND ----------

# MAGIC %md
# MAGIC 생성한 테이블 OrderLines를 조회해보겠습니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC   from OrderLines;

# COMMAND ----------

# MAGIC %md

# COMMAND ----------

# MAGIC %md
# MAGIC ### JDBC 드라이버 사용하여 SQL Server 쿼리
# MAGIC Azure Databricks는 JDBC를 사용하여 외부 데이터베이스에 연결할 수 있도록 지원합니다.  
# MAGIC **`New > Data`** 를 사용하면 SQL Server 뿐 아니라 Postgre, MySQL, MongoDB, kafka 등 다양한 데이터 소스를 Databricks로 데이터를 쉽게 로드할 수 있는 방법을 확인하실 수 있습니다.  
# MAGIC 이번에는 Python, SQL의 예제와 함께 연결을 구성하고 사용하기 위한 기본 구문을 활용해보겠습니다.  

# COMMAND ----------

# MAGIC %md
# MAGIC #### 1. 연결정보  
# MAGIC JDBC를 사용하여 데이터를 읽으려면 여러 설정을 구성해야 합니다. 
# MAGIC 각 데이터베이스는 **`<url>`** 에 다른 형식을 사용하시면 됩니다. 자세한 내용은 [링크](https://learn.microsoft.com/ko-kr/azure/databricks/external-data/jdbc)를 참조해주세요.  
# MAGIC SQL Server 연결을 위해 SQL Server명, 데이터베이스명, 테이블명, SQL Server 사용자 정보를 각 변수에 담습니다. 

# COMMAND ----------

driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
#url = "jdbc:sqlserver://<Server-Name>:1433;DatabaseName=<Database-Name>"
url = "jdbc:sqlserver://adbworkshopsvr.database.windows.net:1433;DatabaseName=WideWorldImporters"
table = "Sales.Customers"
user = "adbadmin"
password = "qwerty7410!"

# COMMAND ----------

# MAGIC %md
# MAGIC #### 2. 데이터 읽기
# MAGIC Python으로 Dataframe을 만들고 위에서 정의한 변수를 참조하여 원격으로 SQL Server의 Database(WideWorldImporters) 내 Customers 테이블을 쿼리합니다.

# COMMAND ----------

df= (
  spark.read.format("jdbc") \
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    # The following options configure parallelism for the query. This is required to get better performance, otherwise only a single thread will read all the data
    # a column that can be used that has a uniformly distributed range of values that can be used for parallelization
    # .option("partitionColumn", "partition_key")
    # lowest value to pull data for with the partitionColumn
    # .option("lowerBound", "minValue")
    # max value to pull data for with the partitionColumn
    # .option("upperBound", "maxValue")
    # number of partitions to distribute the data into. Do not set this very large (~hundreds) to not overwhelm your database
    # .option("numPartitions", <cluster_cores>)
    .load()
)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 3. 데이터 쿼리  
# MAGIC Dataframe에 담긴 데이터 중 CustomerID를 조회해봅니다.

# COMMAND ----------

display(df.select("CustomerID","DeliveryMethodID"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### 4. 테이블 생성  
# MAGIC Dataframe을 테이블로 생성해보겠습니다.

# COMMAND ----------

df.write.format("delta").mode("overwrite").saveAsTable("Customers")

# COMMAND ----------

# MAGIC %md
# MAGIC 생성된 테이블 Customers를 조회합니다.

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Customers
