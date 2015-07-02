Ejercicio Spark
===============

## Contenido

Este proyecto ha usado como base el empleado en clase ([1]) para realizar las tareas de Spark, y ha sido extendido creando
unas nuevas clases en Scala, dentro del paquete `org.utad`:

+ `OlympicMedalsAgeApp` para el primer ejercicio: se aprecian ciertos errores en el fichero de entrada, al descubrirse
que atletas de CERO años de edad consiguieron un total de 5 medallas.

* `OlympicAthleteRankingApp` para el segundo ejercicio

* `OlympicCountryRankingApp` para el tercer ejercicio

* `OlympicsHelper`, librería de utilidades comunes para los ejercicios


## Como ejecutarlo

Cada una de las clases que implementan la solución a los ejercicios extiende a App, con lo que es fácilmente ejecutable.

La forma mas sencilla de ver el resultado es ejecutándolo desde línea de comandos y viendo el resultado por
la salida estándar, para ello habría que ejecutar

    mvn -q clean compile install exec:java -Dexec.mainClass="org.utad.OlympicMedalsAgeApp" # Ejercicio 1

    mvn -q clean compile install exec:java -Dexec.mainClass="org.utad.OlympicAthleteRankingApp" # Ejercicio 2

    mvn -q clean compile install exec:java -Dexec.mainClass="org.utad.OlympicCountryRankingApp" # Ejercicio 3

Añadiendo el parámetro opcional

    -Dlog4j.configuration=file:conf/log4j.properties

conseguimos ver únicamente la salida de los ejercicios

## Librerías

Las librerías que se encuentran en el *pom.xml* son las mismas que usamos en clase para poder compilar y ejecutar
código Scala usando IntelliJ, y ejecutar programas Spark en local.


[1]: https://github.com/aagea/spark-ejercicio "Ejercicio Spark"