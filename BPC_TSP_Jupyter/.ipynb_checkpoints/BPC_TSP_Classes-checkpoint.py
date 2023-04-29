import math

DEFAULT_DEBUG_FILE_NAME = "SparkDebugLog.txt"

def debug_log(message, fname=DEFAULT_DEBUG_FILE_NAME):
    with open(fname, 'a') as f:
        f.write(message)
        
def clear_debug_log(fname=DEFAULT_DEBUG_FILE_NAME):
    open(fname, 'w').close()

class SquareMat:
    def __init__(self, arr, dim):
        self._g = arr
        self.dim = dim
    
    def __getitem__(self, keys):
        row = keys[0]
        col = keys[1]
        
        return self._g[row * self.dim + col]
    
    def __setitem__(self, keys, val):
        row = keys[0]
        col = keys[1]
        self._g[row * self.dim + col] = val
    
    def get_row(self, i):
        row_start = i * self.dim
        row_end = row_start + self.dim
        
        return self._g[row_start:row_end]
    
    def get_col(self, i):
        return [x for idx, x in enumerate(self._g) if idx % self.dim == i]
    
    def make_copy(self):
        return SquareMat(self._g[:], self.dim)
    
    def __reduce__(self):
        return (_SquareMat, (self._g, self.dim))
    
    def __str__(self):
        acc = []
        for i in range(self.dim):
            row = self.get_row(i)
            row = map(lambda x : str(x), row)
            row = "\t".join(row)
            acc.append(row)
        return "\n".join(acc)
        

class BPC_TSP_Graph:
    def __init__(self, edges_grid, dim, vert_weights, limit):
        if type(edges_grid) is SquareMat:
            self.grid = edges_grid
        else:
            self.grid = SquareMat(edges_grid, dim)
        self.dim = dim
        self.vert_weights = vert_weights
        self.limit = limit
        
    # Get data on all adjacent nodes that aren't already visited. Data is: (id, length of edge to the node, weight of the node)
    # NOTE: Is 'removed' param even necessary if reduced replaces all connected edge vals with inf?
    def get_all_adjacent(self, vert_id, removed=set()):
        acc = []
        for i in range(self.dim):
            if i in removed:
                continue
            cur_outgoing = self.grid[vert_id, i]
            if vert_id != i and not math.isinf(cur_outgoing):
                acc.append( (i, cur_outgoing, self.vert_weights[i]) )
        return acc
    
    def get_reduced(self, n, between_paths={},prev_removed=set()):
        g = self.make_copy().grid
        between_paths = between_paths.copy()
        
        for src in range(self.dim):
            if src == n or src in prev_removed:
                continue
            for dest in range(self.dim):
                if dest == n or src == dest or dest in prev_removed:
                    continue
                
                dist_through_n = g[src, n] + g[n, dest]
                if dist_through_n < g[src, dest]:
                    g[src, dest] = dist_through_n
                    # BOOKMARK TODO: Investigate if this part is correct; particularly the append part
                    
                    pstart = between_paths[(src, n)] if (src, n) in between_paths else []
                    pend = between_paths[(n, dest)] if (n, dest) in between_paths else []
                    between_paths[(src, dest)] = pstart + [n] + pend
        
        for src in range(self.dim):
            for dest in range(self.dim):
                if src == n or dest == n:
                    g[src, dest] = math.inf
        
        
        return (BPC_TSP_Graph(g, self.dim, self.vert_weights, self.limit), between_paths) #, prev_removed.copy() + n)
        
            
    
    def __reduce__(self):
        return (BPC_TSP_Graph, (self.grid._g[:], self.dim, self.vert_weights, self.limit))
    
    def make_copy(self):
        return BPC_TSP_Graph(self.grid.make_copy(), self.dim, self.vert_weights, self.limit)
    
    def get_col(self, i):
        return self.grid.get_col(i)
            

class SearchNode:
    def __init__(self, vert, path, visited, profit_earned, remaining_capacity, reduced_graph=None, upper_bound=None):
        self.vertex = vert
        self.path = path
        if type(visited) is set:
            self.visited = visited
        else:
            self.visited = set(visited)
        self.profit = profit_earned
        self.capacity = remaining_capacity
        self.bound = upper_bound
        self.reduced_graph = reduced_graph
    
    def set_bound(self, new_bound):
        self.bound = new_bound
    
    def __reduce__(self):
        return (SearchNode, (self.vertex, self.path, self.path, self.profit, self.capacity, self.reduced_graph, self.bound))
    
    def __lt__(self, b):
        # Just to keep the interpreter happy when bounds are equal.
        # Comparisons normally handled by first element in tuple,
        # but if first elements are equal, then queue will compare
        # second elements instead. We don't care about order when bounds
        # are equal, so this will just return True.
        return True




        
def minbound_evaluate(graph, sn):
    def sort_for_eval(g, s):
        non_z_min = lambda r : min([x for x in r if x != 0])

        grid_data = s.reduced_graph if s.reduced_graph is not None else g.grid
        grid = grid_data[0].make_copy()

        min_inbounds = [non_z_min(row) for row in [grid.get_col(idx) for idx in range(grid.dim)]]
        weights = zip(min_inbounds, grid.vert_weights)
        weights = [w for w in weights if not math.isinf(w[0])]
        processed_weights = [((w[1] / w[0]), w) for w in weights]
        processed_weights.sort(reverse=True, key=lambda x : x[0])
        ret_weights = [w[1] for w in processed_weights]
        return ret_weights
    
    
    unvisited_weights = sort_for_eval(graph, sn)
    unvisited_weights = [x for x in unvisited_weights if x not in sn.visited]

    prof = sn.profit
    cap = sn.capacity

    for w in unvisited_weights:
        if w[0] > cap:
            prof += (cap / w[0]) * w[1]
            cap = 0
            break
        else:
            prof += w[1]
            cap -= w[0]

    return prof
                
#         @staticmethod
#         def setup(g, sn):
#             non_z_min = lambda r : min([x for x in r if x != 0])
#             min_inbounds = [non_z_min(row) for row in [g.grid.get_col(idx) for idx in range(g.dim)]]
#             weights = list(zip(min_inbounds, g.vert_weights))
#             processed_weights = [((w[1] / w[0]), w) for w in weights]
#             processed_weights.sort(reverse=True, key=lambda x : x[0])
#             ret_weights = [w[1] for w in processed_weights]
#             return ret_weights
        
#         @staticmethod
#         def re_sort(weights, rgrid_data):
            
        
#         @staticmethod
#         def evaluate(weights, sn):
#             unvisited_weights = [w for vert_id, w in enumerate(weights) if vert_id not in sn.visited]
            
#             prof = sn.profit
#             cap = sn.capacity
            
#             for w in unvisited_weights:
#                 if w[0] > cap:
#                     prof += (cap / w[0]) * w[1]
#                     cap = 0
#                     break
#                 else:
#                     prof += w[1]
#                     cap -= w[0]
            
#             return prof
            

#         @staticmethod
#         def min_inbound_fractional(g, sn):
#             visited = sn.visited
#             print(f"visited: {visited}")
#             non_z_min = lambda r : min([x for x in r if x != 0])
#             min_inbounds = [non_z_min(row) for row in [g.grid.get_row(idx) for idx in range(g.dim)]]
#             print(f"min_inbounds: {min_inbounds}")
#             weights = list(zip(min_inbounds, g.vert_weights))
#             print(f"weights: {weights}")
#             filtered_weights = [((w[1] / w[0]), w) for vid, w in enumerate(weights) if vid not in visited]
#             print(f"filtered_weights: {filtered_weights}")
#             filtered_weights.sort(reverse=True, key=lambda x : x[0])
#             print(f"filtered_weights (sorted): {filtered_weights}")
#             boundcheck_list = [x[1] for x in filtered_weights]
#             print(f"boundcheck_list: {boundcheck_list}")

#             prof = sn.profit
#             cap = sn.capacity
#             for w in boundcheck_list:
#                 if w[0] > cap:
#                     prof += (cap / w[0]) * w[1]
#                     cap = 0
#                     break
#                 else:
#                     prof += w[1]
#                     cap -= w[0]

#             return prof
        

def brute_force_evaluate(_u, _s):
    return math.inf