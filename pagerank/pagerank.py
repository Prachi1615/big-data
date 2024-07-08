from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("PageRank").getOrCreate()
edges = spark.read.csv("gs://spark_pg/input/graph_edges.csv", header=True, inferSchema=True)
edges_rdd = edges.rdd.map(lambda row: (row['src'], row['dst']))
ranks = edges_rdd.flatMap(lambda edge: (edge[0], edge[1])).distinct().map(lambda node: (node, 1.0))
iterations = 10
for iteration in range(iterations):
    contributions = edges_rdd.join(ranks).flatMap(lambda edge_rank: [(edge_rank[1][0], edge_rank[1][1]/2)])
    ranks = contributions.reduceByKey(lambda x, y: x + y).mapValues(lambda rank: 0.15 + 0.85*rank)
final_ranks = ranks.collect()
for node, rank in final_ranks:
    print(f"{node} has rank: {rank}")
spark.stop()
