//By Wanyanyuan Tang and Mounia Bouhriz
import org.graphframes.GraphFrame
//Q1
val file = sc.textFile("/user/hive/warehouse/new_auth_10000000.txt") 
//Q2
val cleanfile = file.filter(line => !(line.contains("?")))
//Q3
val trifile = cleanfile.map(line=>line.split(",")).map(fields=>((fields(1),fields(3)),1)).reduceByKey((v1,v2) => v1+v2)
//Q4
cleanfile.map(line=>line.split(",")).map(fields=>((fields(1),fields(3)),1)).reduceByKey((v1,v2) => v1+v2).sortBy(_._2,false).take(10)
//Q5
trifile.saveAsTextFile("/home/cloudera/result")
//Q6
val userfile = file.map(line=>line.split(",")).map(fields=>((fields(1),"utilisateur"))) 
val userfileg=userfile.toDF("id","type")
val machinefile = file.map(line=>line.split(",")).map(fields=>((fields(3),"machine")))
val machinefileg=machinefile.toDF("id","type")
val totalfile=userfileg.unionAll(machinefileg)
val v=totalfile.toDF("id", "type").select("id","type").distinct()
val newtrifile = trifile.map(fields=>(fields._1._1, fields._1._2, fields._2))
val e=newtrifile.toDF("src","dst","weight")
val g = GraphFrame(v,e) 
g.vertices.show()
g.edges.show()
//Q7
val indre=g.inDegrees.sort(desc("inDegree"))
val outdre=g.outDegrees.sort(desc("outDegree"))
indre.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/indre")
outdre.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/outdre")
//Q8
val component = g.connectedComponents.run()
component.select("id", "component").orderBy("component").show()
component.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/component")
//Q9
val result = g.stronglyConnectedComponents.maxIter(10).run()
result.select("id", "component").orderBy("component").show()
result.rdd.map(_.toString()).saveAsTextFile("/home/cloudera/result")
//Q10
val results = g.pageRank.resetProbability(0.15).tol(0.01).run()
results.vertices.select("id", "pagerank").show()
results.edges.select("src", "dst", "weight").show()
