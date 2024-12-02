# Probabilistic ID Unification Clustering

import networkx as nx
from scipy.cluster import hierarchy
import scipy.spatial.distance as ssd
import pandas as pd
import numpy as np
from fancyimpute import SoftImpute



def fill_missing_links(matrix, convergence_threshold=0.01):
    """
    Fills missing values in the adjacency matrix using the SoftImpute algorithm.
    If there are no missing values, the original matrix is returned.
    SoftImpute is used to impute missing values in a way that respects the similarity structure of the data.

    Args:
        matrix: adjacency matrix
        convergence_threshold: convergence threshold for SoftImpute algorithm

    Returns:
        Numpy adjacency matrix with imputed missing values
    """
    matrix_ = matrix.copy()
    np.fill_diagonal(matrix_, 1)
    matrix_[matrix_ == 0] = np.nan
    if not np.isnan(matrix_).any():
        return matrix

    imputer = SoftImpute(min_value=0, max_value=1, verbose=False, convergence_threshold=convergence_threshold,
                         init_fill_method='mean')  # init_fill_method='mean' significantly improves speed
    matrix_ = imputer.fit_transform(matrix_)
    # the adjacency matrix needs to have zeros on the diagonal
    np.fill_diagonal(matrix_, 0)

    # force symmetry
    matrix_ = np.tril(matrix_) + np.triu(matrix_.T, 1)
    return matrix_

def clusters(data, ROW_ID, DEDUPLICATION_ID_NAME, cluster_threshold, convergence_threshold, col_names, fill_missing):
    """
    --> Performs hierarchical clustering on the input data and assigns deduplication IDs to clusters.
    --> Uses a graph representation where nodes are identifiers and edges are similarity scores.
    --> Connected components (clusters) are identified in the graph.
    --> For each cluster, a subgraph is created, and hierarchical clustering is applied to assign deduplication IDs.
     --> The resulting deduplication IDs are returned in a DataFrame

    Args:
        data: input DataFrame containing similarity scores and identifiers

        ROW_ID: column name for unique identifiers

        DEDUPLICATION_ID_NAME: column name for deduplication IDs

        cluster_threshold: similarity threshold for clustering

        convergence_threshold: convergence threshold for SoftImpute in case of missing values

        col_names: list of column names used in clustering

        fill_missing: boolean indicating whether to fill missing values in the adjacency matrix

    Returns:
        DataFrame with deduplication IDs assigned to clusters
    """
    # Create an undirected graph
    graph = nx.Graph()

    # Add nodes and edges to the graph
    for j, row in data.iterrows():
        graph.add_node(row[f'{ROW_ID}_1'], **{col: row[f'{col}_1'] for col in col_names})
        graph.add_node(row[f'{ROW_ID}_2'], **{col: row[f'{col}_2'] for col in col_names})
        graph.add_edge(row[f'{ROW_ID}_1'], row[f'{ROW_ID}_2'], score=row['score'])

    # Find connected components (clusters) in the graph
    components = nx.connected_components(graph)

    # Initialize variables for clustering
    clustering = {}
    cluster_counter = 0

    # Process each connected component
    for component in components:
        subgraph = graph.subgraph(component)

        # Check if the subgraph has more than one node
        if len(subgraph.nodes) > 1:
            adjacency = nx.to_numpy_array(subgraph, weight='score')

            # Fill missing values in the adjacency matrix if specified
            if fill_missing:
                adjacency = fill_missing_links(adjacency, convergence_threshold)

            # Calculate distances and perform hierarchical clustering
            distances = (np.ones_like(adjacency) - np.eye(len(adjacency))) - adjacency
            condensed_distance = ssd.squareform(distances)
            linkage = hierarchy.linkage(condensed_distance, method='centroid')
            clusters = hierarchy.fcluster(linkage, t=1 - cluster_threshold, criterion='distance')

        else:
            clusters = np.array([1])

        # Update clustering dictionary with deduplication IDs
        clustering.update(dict(zip(subgraph.nodes(), clusters + cluster_counter)))
        cluster_counter += len(component)

    # Create a DataFrame with deduplication IDs and sort by the IDs
    df_clusters = pd.DataFrame.from_dict(clustering, orient='index', columns=[DEDUPLICATION_ID_NAME])
    df_clusters.sort_values(DEDUPLICATION_ID_NAME, inplace=True)
    df_clusters[ROW_ID] = df_clusters.index


    return df_clusters