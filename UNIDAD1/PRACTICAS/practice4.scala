import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema","true")csv("BIG-DATA/BigData-master/Spark_DataFrame/CitiGroup2006_2008")

df.printSchema()

//1.avg(): devuelve el promedio de los valores en un grupo.
df.select(avg("Open")).show()
//2.Collect_list(): devuelve una lista de objetos con duplicados.
df.select(collect_list("Open")).show()
//3.corr(): devuelve el coeficiente de correlación de Pearson para dos columnas.
df.select(corr("High", "Low")).show()
//4.count(): devuelve el número de elementos en un grupo.
df.select(count("Low")).show()
//5.countDistinct(): devuelve el número de elementos distintos en un grupo.
df.select(countDistinct("Low")).show()
//6.first(): Devuelve el primer valor de una columna en un grupo
df.select(first("High")).show()
//7.last(): Devuelve el ultimo valor de una columna en un grupo
df.select(last("High")).show() 
//8.max(): devuelve el valor maximo de una columna en un grupo
df.select(max("Close")).show()
//9.mean(): Hace la misma funcion que avg, devuelve la media de los valores de una columna en un grupo
df.select(mean("Open")).show()
//10.min(): devuelve el valor minimo de una columna en un grupo
df.select(min("Close")).show()
//11.stddev_pop(): devuelve la desviacion estandar de una expresion en un grupo
df.select(stddev_pop("Low")).show()
//12.sum(): devuelve la suma de todos los valores en la expresion
df.select(sum("High")).show() 
//13.sumDistinct(): devuelve la suma de los valores distintos en la expresion
df.select(sumDistinct("High")).show() 
//14.sort(asc()): ordena de forma ascendente los elementos de una columna
df.sort(asc("High")).show(5) 
//15.sort(desc()): ordena de forma descendente los elementos de una columna
df.sort(desc("High")).show(5) 
//16.covar_pop(): devuelve la covarianza de la población para dos columnas.
df.select(covar_pop("High", "Low")).show()
//17.kurtosis(): devuelve la curtosis de los elementos de una columna
df.select(kurtosis("High")).show()
//18.skewness(): devuelve el sesgo de los valores en un grupo. 
df.select(skewness("High")).show()
//19.year(): devuelve el anio de la columna fecha indicada
df.select(year($"Date")).show(5)
//20.month(): devuelve el mes de la columna fecha indicada
df.select(month($"Date")).show(5)