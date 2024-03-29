
//Examen de Datos Masivos 
//Unidad 2
// MUltilayer perceptron
import org.apache.spark.ml.classification.MultilayerPerceptronClassificationModel
import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.feature.IndexToString
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession

//Se carga la data de Iris que será el DataFrame
val df = spark.read.option("header", "true").option("inferSchema","true")csv("BIG-DATA/BigData-master/Spark_DataFrame/Iris.csv")

// Se imprime esquema
df.printSchema()

//Renombrar columnas
val newnames = Seq("SepalLength","SepalWidth","PetalLength","PetalWidth","Species")

//DataFrames nuevos con las cabeceras renombradas
val dfRenamed = df.toDF(newnames: _*)

//Le decimos que seleccione las columnas y asigne Species  como label
val data = dfRenamed.select($"SepalLength",$"SepalWidth",$"PetalLength",$"PetalWidth",$"Species".as("label"))

//Juntamos las columnas que serán features (caracteristicas) en una sola columna
val assembler = new VectorAssembler()
.setInputCols(Array("SepalLength", "SepalWidth", "PetalLength", "PetalWidth"))
.setOutputCol("features")
//Transformamos la data para que quede la columna
val features = assembler.transform(data)
//Encontramos las etiquetas que se encuentran en label para saber cuantos tipos de clases hay
//Agregamos todas las etiquetas en el indixe
val labelIndexer = new StringIndexer().setInputCol("label").setOutputCol("indexedLabel").fit(features)
println(s"Found labels: ${labelIndexer.labels.mkString("[", ", ", "]")}")
//Identificamos las caracteristicas de la columna features
val featureIndexer = new VectorIndexer().setInputCol("features").setOutputCol("indexedFeatures").setMaxCategories(4).fit(features)
// Dividimos los datos, 70% (105 datos) para entrenar el modelo y 30% (45 datos) para evaluarlo
val splits = features.randomSplit(Array(0.7, 0.3))
val trainingData = splits(0)
val testData = splits(1)

//Diseno de la arquitectura
//tres neuronas de entrada, dos en la capa oculta
val layers = Array[Int](4, 5, 5, 4)
//Creamos el entrenamiento y ajustamos los parametros
val trainer = new MultilayerPerceptronClassifier()
.setLayers(layers)
.setLabelCol("indexedLabel")
.setFeaturesCol("indexedFeatures")
.setBlockSize(128)
.setSeed(System.currentTimeMillis)
.setMaxIter(200)

//Convertimos las etiquetas indexadas de nuevo en las etiquetas originales
//Y para tener una columna que mencionará las predicciones
val labelConverter = new IndexToString()
.setInputCol("prediction")
.setOutputCol("predictedLabel")
.setLabels(labelIndexer.labels)
//colocamos los indexadores de cadena en un pipeline
val pipeline = new Pipeline()
.setStages(Array(labelIndexer, featureIndexer, trainer, labelConverter))
//Hacemos fit y entrenamos
val model = pipeline.fit(trainingData)
//Tomamos la data de prueba y hacemos predicciones
val predictions = model.transform(testData)
//Mostramos las predicciones
predictions.show(5)

//Calculamos el error y vemos que tan exacto es nuestro modelo
val evaluator = new MulticlassClassificationEvaluator()
.setLabelCol("indexedLabel")
.setPredictionCol("prediction")
.setMetricName("Precision")
val accuracy = evaluator.evaluate(predictions)
println("Porcentaje de Error = " + (1.0 - accuracy))
