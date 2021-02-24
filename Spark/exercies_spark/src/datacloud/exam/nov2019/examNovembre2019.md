# Examen réparti 1 DataCloud Nov 2019

## Exercice 1 – Questions de cours
### Q1
Donnez les 3 V fondamentaux du Big Data
```js
   // Volue , Vitesse et Variété
```
### Q2
A quoi sert un partitioneur  ? Donnez un exemple.
```js
   // ça définit la politique de répaartition en sortie des maps , hachage de clé 
```
### Q3
Le HDFS privilège-t-il le débit ou la latence ? Justifiez
```js
   //Le débit car lecture séquentielle
```
### Q4
Quels sont les mécanismes du HDFS pour détecter et réparer les crashes
- d’un DataNode
- du NameNode
```js
   //DataNode : 

   //NameNode :

```
### Q5
Dans YARN quel mécanisme empêche une application d’utiliser des conteneurs qui ne lui appar-tiennent pas.
```js
   //aucune idée 
```
### Q6
Définir la curryfication.
```js
   //c'est la  transformation d'une fonction à plusieurs arguments en une fonction à un argument qui va retourne une fonction sur le reste des arguments
```
### Q7
Soit le code scala suivant :
```Scala
object Truc {
    var x = 42
    def f(a:Int, b:Int)=a*x+b
}
```
La fonction f est-elle pure ? Justifiez
```js
   //non car la fonction f modifie une valeur qui n'est pas dans sont contexte 
```

## Exercice 2 – PageRank
### Q1

Donnez les types des variables lines, links, ranks, join, values et tmp.
```Scala
def pageRank(file:String, out:String, iters: Int, spark: SparkContext)
={
			val lines = spark.textFile(file)
					val links = lines.map(_.split(" "))
					.map(a=> (a(0),a(1)))
					.distinct()
					.groupByKey()
					links.cache()
					var ranks = links.mapValues(v => 1.0)
					for (i <- 1 to iters) {
						val join = links.join(ranks)
								val values = join.map(_._2)
								val tmp = values.flatMap(
								urls_rank => urls_rank._1.map(
				                url => (url, urls_rank._2 
                                            / urls_rank._1.size))
											)
						ranks = tmp.reduceByKey(_ + _)
					}
		ranks.saveAsTextFile(out)
}
```
lines
```js
   //RDD[String]
```
links
```js
    //RDD[(String , Iterable<String>)]  
```
ranks
```js
   //RDD[(String , Double)] 
```
join
```js
   // RDD[(String ,(Iterable<String> , Double))]  
```
values
```js
   // RDD[(Iterable<String> , Double)]
```
tmp
```js
   // RDD[(String, Double)]
```

### Q2
La méthode cache() est équivalent à persist(StorageLevel.MEMORY_ONLY). Pourquoi est-ce
judicieux de l’appliquer sur la variable links ?
```js
   // links va etre iters fois la mettre en cache permet un gain en performance 
```

### Q3
Nous supposons que le fichier d’entrée fait 300 Mo et qu’il est stocké sur un HDFS avec une
configuration par défaut (taille bloc = 128 Mo et facteur de réplication = 3). Pour rappel, la
méthode textfile utilise exactement le même algorithme de découpage que Hadoop MapReduce
(i.e. la classe TextInputFormat). Donnez le nombre de partitions de lines. Justifiez.

```js
   // aucune idée  
```
### Q4
Donnez le graphe de tâches de l’appel à cette fonction pour un fichier de 300 Mo et pour 2
itérations. Vous justifierez votre réponse en donnant au préalable le graphe de dépendance des
RDDs en y faisant apparaître les différents stages.
N.B. : il est conseillé de faire un brouillon.
```js
   // aucune idée
```
## Exercice 2 – Usagers des transports
### Q1
Afin de pouvoir de pouvoir quantifier et localiser l’affluence des usagers, on souhaite connaître
pour chaque station et pour chaque tranche horaire d’une heure le nombre d’usagers qui ont
badgé dans cette station à cette tranche horaire. Pour cela on propose d’écrire la fonction
nbUsersByStationAndByhour qui prend en paramètre le chemin HDFS des logs et un SparkCon-
text et qui retourne un RDD qui contient le résultat attendu. Écrivez cette fonction en précisant
le type de retour de son résultat dans son en-tête.
```Scala
   def nbUsersByStationAndByhour  (file:String, spark: SparkContext) {

   }
```
### Q2
Que faudrait-il modifier à la question précédente pour faire en sorte que chaque partition du
RDD retourné ne soit associée qu’à une station ?
```js
   // aucune idée
```
### Q3
On souhaite maintenant coder la fonction proportionTicketCarteByStation qui prend en pa-
ramètre le chemin HDFS des logs et un SparkContext et qui produit un RDD associant pour
chaque station en pourcentage la proportion d’usagers qui ont utilisé un ticket et la proportion
d’usagers qui ont utilisé une carte d’abonnement (pour une station, la somme des deux valeurs
doit donc être égale à 100). Écrivez cette fonction en précisant le type de retour de son résultat
dans son en-tête.
```js
   // aucune idée
```
### Q4
En appelant la méthode de la question précédente écrire le programme d’une application Spark
qui affiche sur la sortie standard la station de métro qui a la différence de proportion la plus
grande entre les tickets et les cartes d’abonnement.
On suppose désormais que chaque ligne de log produite est envoyée vers un système d’agrégation
de logs qui peuvent être retransmises ensuite de manière continue vers notre système Spark. On
supposera que la source d’émission du système d’agrégation est la machine stat.ratp.fr sur le port
7815 et que l’on accédera à ce flux de log via une socket TCP.
```js
   // aucune idée
```
### Q5
On souhaite connaître une estimation du nombre d’usagers actuellement présents dans le métro.
Puisque l’on connaît uniquement la date d’entrée de chaque usager (mais pas sa date de sortie) on
considère qu’un usager reste en moyenne 45 minutes dans le métro. Écrire un programme Spark
qui affiche toutes les 30 secondes cette estimation. Vous ferez en sorte d’utiliser les primitives les
plus adéquates afin de maximiser les performances.
```js
   // aucune idée
```
### Q6
Sur le même principe que la question 3, on souhaite avoir en temps réel la proportion de tickets
et de cartes d’abonnement qui ont badgé par station depuis le début du flux.
```js
   // aucune idée
```