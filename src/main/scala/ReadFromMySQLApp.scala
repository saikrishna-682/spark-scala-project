import org.apache.spark.SparkContext.getOrCreate
import org.apache.spark.sql.{DataFrame, SparkSession}

object ReadFromMySQLApp {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("ReadFromMySQL")
      .config("spark.master", "local")
      .config("spark.jars", "C:\\Users\\muppa\\Downloads\\mysql-connector-j-8.2.0.jar")
      .getOrCreate()

    // MySQL connection properties
    val url = "jdbc:mysql://localhost:3306/testdb"
    val properties = new java.util.Properties()
    properties.setProperty("user", "root")
    properties.setProperty("password", "root")
    properties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    // Table name
    val tableName = "trades"

    // Read data from MySQL table
    val data: DataFrame = spark.read
      .jdbc(url, tableName, properties)

    // Show the data
    data.show()

    // Stop the Spark session
    spark.stop()
  }
}
