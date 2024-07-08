import pandas as pd

data = {
    'src': ['A', 'A', 'B', 'C'],
    'dst': ['B', 'C', 'C', 'A']
}

df = pd.DataFrame(data)

df.to_csv('graph_edges.csv', index=False)
