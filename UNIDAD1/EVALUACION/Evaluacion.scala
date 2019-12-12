//EVALUACION UNIDAD 1
//Muestras de datos
val sample0 = List(10,5,20,20,4, 5, 2, 25, 1)
val sample1 = List(3,4,21,36,10,28,35,5,24,42)

//Función breakingRecords
def breakingRecords(scores:List[Int]): Unit =
{
    var min = scores(0) 
    var max = scores(0)
    var cont_min = 0
    var cont_max = 0

    // Ciclo for para realizar comparativas 
     for (i <- scores) 
    {
        if (i<min)
        {
            min = i 
            cont_min = cont_min +1
        }
        else if (i>max) 
        {
            max = i 
            cont_max = cont_max + 1
        }
    }
    println("Output:")
    println (cont_max, cont_min)
}

//Llamada para salida
breakingRecords(sample0)

breakingRecords(sample1)