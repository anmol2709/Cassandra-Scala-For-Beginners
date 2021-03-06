import com.datastax.driver.core._
import org.apache.log4j.Logger

object CassandraScalaDemo extends App {

  val log = Logger.getLogger(this.getClass)

  //creating Cluster object
  val cluster = Cluster.builder().addContactPoint("localhost").withQueryOptions(new QueryOptions().setConsistencyLevel(ConsistencyLevel.ONE)).build()

  //Creating Session object
  val session = cluster.connect()

Thread.sleep(2000)
  val consistencylevel = cluster.getConfiguration.getQueryOptions.getConsistencyLevel()
    log.info("Current consistency level is  " + consistencylevel )

  //Executing the query for creating KeySpace
  val createKeySpaceQuery = "CREATE KEYSPACE cassandraDemo WITH replication = {'class':'SimpleStrategy', 'replication_factor':1};"
  session.execute(createKeySpaceQuery)
  log.info("Keyspace created")
  Thread.sleep(2000)

  //Using the KeySpace
  session.execute("USE cassandraDemo")
  log.info("Keyspace entered")
  Thread.sleep(2000)

  //Creating the Table
  val createTableQuery = "CREATE TABLE employee" +
    "(emp_id int, emp_name text,emp_city text,emp_salary varint" +
    ",emp_phone varint,PRIMARY KEY(emp_id,emp_salary));"
  session.execute(createTableQuery)
  log.info("Table created")

  Thread.sleep(2000)
  //Just waiting for creating the tables to perform insert operation

  // Insert Queries
  val insertQuery1 = "INSERT INTO employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(1,'Name1','Delhi',50000,9876543210) IF NOT EXISTS"
  val insertQuery2 = "INSERT INTO employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(2,'Name2','Noida',70000,987654211) IF NOT EXISTS"
  val insertQuery3 = "INSERT INTO employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(3,'Name3','Faridabad',20000,9876543212) IF NOT EXISTS"
  val insertQuery4 = "INSERT INTO employee(emp_id, emp_name,emp_city ,emp_salary ,emp_phone) VALUES(4,'Name4','Gurgaon',80000,9876543213) IF NOT EXISTS"
  session.execute(insertQuery1)
  session.execute(insertQuery2)
  session.execute(insertQuery3)
  session.execute(insertQuery4)

  //Indexing Helps Perform Select Query on coloumns other than partitioning column
  val createIndexQuery = "CREATE INDEX emp_name ON employee (emp_name);"
  session.execute(createIndexQuery)
  log.info("Index created")

  Thread.sleep(2000)

  //  SELECT QUERIES

  //Basic Select Query For   Rows
  val selectQuery1 = "Select * from employee"
  val res = session.execute(selectQuery1)
  log.info(":::  Select all Query :::")
  res.forEach(log.info(_))

  //Select query using Partitioning key
  val selectQuery2 = "Select * from employee WHERE emp_id=1"

  val res1 = session.execute(selectQuery2)
  log.info(":::  Select all with Emp_id = 1 ::: ")
  res1.forEach(log.info(_))

  //Select query using Partitioning key and Clustering column
  val selectQuery3 = "Select * from employee where emp_id=1 and emp_salary >30000"
  val res3 = session.execute(selectQuery3)
  log.info(":::  Select all with Emp_id = 1 and Salary>30000  ::: ")
  res3.forEach(log.info(_))

  //Select query using Partitioning key and Index
  val selectQuery4 = "Select * from employee where emp_name='Name1'"
  val res4 = session.execute(selectQuery4)
  log.info(":::  Select all with Emp_id = 1  using index name::: ")
  res4.forEach(log.info(_))

  val updateQuery = "UPDATE employee SET emp_name = 'Name5' WHERE emp_id =1 AND emp_salary =50000"
  session.execute(updateQuery)
  log.info("Row Updated")

  Thread.sleep(2000)

  val updateQuery2 = "UPDATE employee SET emp_name = 'Name6' WHERE emp_id =6 AND emp_salary =50000"
  session.execute(updateQuery2)
  log.info("Row Updated/Added")
  Thread.sleep(2000)

//COMMENT THESE IF YOU NEED TO KEEP THE DATA IN CASSANDRA 

  //Removing the Data From the Table
  val truncateQuery = "Truncate table employee"
  session.execute(truncateQuery)
  log.info("Table truncated")

  //Dropping the Table
  val dropTableQuery = "DROP TABLE employee"
  session.execute(dropTableQuery)
  log.info("Table dropped")

 val dropKeyspace = "DROP KEYSPACE cassandrademo"
  session.execute(dropKeyspace)
  log.info("Keyspace dropped")

  //Closing the cluster
  cluster.close()
}
