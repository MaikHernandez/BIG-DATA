import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val df = spark.read.option("header", "true").option("inferSchema","true")csv("BIG-DATA/BigData-master/Spark_DataFrame/CitiGroup2006_2008")

df.printSchema()

//df.head(2)

//df.show()

//df.select(month(df("Date"))).show()

//df.select(year(df("Date"))).show()

//val df2 = df.withColumn("Month", month(df("Date")))

//val dfavgs = df2.groupBy("Month").mean()

//dfavgs.select($"Month", $"avg(Close)").sort("Month").show()

val df2mes = df.withColumn("Month",month(df("Date"))) //creacion de la columna mes
val avgmes = df2mes.select($"Month",$"Close").groupBy("Month").mean() ///optenemos tres columnas que es el mes y con avg sacamos el promedio de cada fila
avgmes.select($"Month",$"avg(Close)").orderBy("Month").show()

//val dfmins = df2.groupBy("Year").min()

//dfmins.select($"Year", $"min(Close)").show()