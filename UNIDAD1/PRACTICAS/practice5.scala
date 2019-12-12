import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
val df = spark.read.option("header", "true").option("inferSchema","true")csv("BIG-DATA/BigData-master/Spark_DataFrame/Sales.csv")

df.printSchema()
df.show()

//1. Ventas mayores a 300
df.filter($"Sales">340).show()
//2. Mostrar solo el nombre de la empresa y las ventas correspondientes
df.select("Company", "Sales").show()
//3.Mostrar ventas mayores a 100, pero menores que 500
df.filter($"Sales">100 && $"Sales"<500).show()
//4.Muestra la cantidad de registros por empresa
df.groupBy("Company").count().show() 
//5.Muestra la cantidad de registros por persona
df.groupBy("Person").count().show() 
//6.Muestra el maximo de ventas agrupado por empresa
df.groupBy("Company").max().show()
//7.Muestra el minimo de ventas agrupado por empresa
df.groupBy("Company").min().show() 
//8.Muestra la suma de las ventas agrupado por empresa
df.groupBy("Sales").sum().show()
//9.Muestra el promedio de las ventas
df.select(avg("Sales")).show()
//10.Muestra las ventas iguales a 340
 df.filter($"Sales"===340).show()
//11.Muestra una coleccion de datos de las ventas
 df.select(collect_set("Sales")).show()
//12.Muestra el metodo de tabla cruzada para mostrar una vista tabular de la puntuación por ventas
 val sales = df.filter($"Sales">=400 && $"Company">="FB").stat.crosstab("Sales","Company").show(5)
//13.Muestra las ventas de forma ascendente
df.sort(asc("Sales")).show(5) 
//14.Muestra las empresas en una lista, aun si se repiten
df.select(collect_list("Company")).show()
//15.Muestra las personas en una lista, aun si se repiten
df.select(collect_list("Person")).show()
//16.Muestra el porcentaje del tiempo en que la columna “Sales” fue mayor que $ 300
val porcentaje: Double = ((df.filter($"Sales">300).count())*100.0)/df.count()
printf("El porcentaje es: %1.2f", porcentaje, "%")
println(" %")
//17.Muestra la suma de todas las ventas de las diferentes empresas y personas
df.select(sum("Sales")).show()
//18.Muestra el promedio de las ventas agrupado por empresa
df.groupBy("Company").avg().show()
//19.Muestra el promedio de las ventas agrupado por personas
df.groupBy("Person").avg().show() 
//20.Muestra el numero de registros de cada empresa
df.groupBy($"Company").agg(count("*")).show() 



