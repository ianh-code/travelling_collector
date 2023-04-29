# travelling_collector

__By Ian Hurd__

A repo for studying the budgeted prize-collecting salesperson problem with weighted nodes, a graph problem.

## Old_Attempts

This directory contains abandoned python and scala code used in early attempts to solve this problem.

## BPC_TSP_Jupyter

This is the main directory of the project. It contains all of the working code.

## Requirements

You will need to install pyspark and jupyter in order to run this code.

## Program Functionality

This program generates a series of graphs to solve based on configuration and then solves all that are generated, timing each one. It solves each graph twice, once with a branch-and-bound algorithm, and once with a brute force algorithm (the brute force is the same as the branch and bound except the bounds checking function always outputs infinity as the upper bound, ensuring the tree does not get pruned.). The program then graphs the data.

## How to run

Go to BPC_TSP_Jupyter and open BPC_TSP.ipynb. You may then run the cells. If you want to use a different json file to configure a run, change the global constant DATA_JSON to the name of the desired json file.

## Configuration

Here is an example of a json configuration file:

```
{
    "file_defined":
    [
        {
            "edge_w": "graph1edges.csv",
            "vert_w": "graph1verts.csv",
            "lim": 22,
            "start": 1
        }
    ],
    "randomized":
    [
        {
            "num_verts": 7,
            "percent_unconnected": 25,
            "max_edge_weight": 20,
            "max_vert_weight": 17,
            "cap_range":
            {
                "policy": "minsum",
                "attrs":
                {
                    "lower_multiplier": 1,
                    "upper_multiplier": 1.5
                }
            },
            "num_generations": 1,
            "outputs": null,
            "start": null
        },
        {
            "num_verts": 8,
            "percent_unconnected": 33,
            "max_edge_weight": 15,
            "max_vert_weight": 15,
            "cap_range":
            {
                "policy": "minsum",
                "attrs":
                {
                    "lower_multiplier": 1,
                    "upper_multiplier": 1.5
                }
            },
            "num_generations": 1,
            "outputs": null,
            "start": null
        },
    ]
}
```

The json file is split into two lists. Each element in one of these lists is responsible for generating one or more graphs to be solved. Each dict in the file_defined list generates a single graph from a pair of CSV files. The attributes of each dict are as follows:

- **edge_w:** A CSV file containing a square matrix, with each row representing a source vertex for an edge and each column representing a destination vertex, with the number of a given cell representing the weight of the edge from its row's source to its column's destination. Although indeces are not explicitly listed, rows and columns are zero-indexed, with their indexes as the names of their respective nodes. An edge value of i means infinite length, which essentially means there are no edges from that source to that destination.

- **vert_w:** A CSV file containing a single row of values. Each value represents the profit at a given node. This list is also to be considered zero-indexed, with indeces matching up to indexes in the edge-weight matrix.

- **lim:** The limit on edge weight accrued on a traversal

- **start:** Was supposed to be the index of the starting node of a traversal, but the starting node is currently hardcoded as 0.

Each dict in the "randomized" list, on the other hand, specifies constraints on randomly-generating a graph or graphs. Here are the attributes of a dict in the "randomized" list

- **num_verts:** The number of vertices the randomly-generated graph will have.

- **percent_unconnected:** A number between 0 and 100. This specifies the percent odds of any given pair of nodes not being connected by an edge. This is represented by a value of inf (infinity).

- **max_edge_weight:** The upper possible bound of a randomly-generated edge weight that is not inf. The lower bound is always 1 (inclusive)

- **max_vert_weight:** The upper possible bound of a randomly-generated vertex profit that is not inf. The lower bound is always 1 (inclusive)

- **cap_range:** Specifies how a weight limit, or capacity, is decided upon. This is in itself its own dict. The attributes are described below.

    - **policy:** Was meant to specify a named policy on how the capacity is decided upon, however currently, the only policy is "minsum", which uses the sum of all minimum inbound edges to each vertex to decide upon a capacity.

    - **attrs:** Contains further attributes specific to each different policy to further constrain the generation of the capacity. For minsum, the lower and upper bounds of random capacity generation are derived from these attributes, and the sum of minimum inbound edges:
    
        - **lower_multuplier:** The lower bound is lower_multiplier times the calculated minimum sum.

        - **upper_multiplier:** The upper bound is upper_multiplier times the calculated minimum sum.

- **num_generations:** The number of random graphs to generate with this specific configuration.

- **Outputs:** Currently unused.

- **start:** Was supposed to be the index of the starting node of a traversal, but the starting node is currently hardcoded as 0.

## Additional Output

Beyond the output to the notebook itself, the program also outputs to files "solved_brute_force.txt" and "solved_minbound.txt", to which all the generated graphs and their solutions are recorded. The notebook prints some of the graphs, but the primary output there is performance metrics.