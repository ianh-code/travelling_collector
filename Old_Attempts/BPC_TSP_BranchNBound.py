import random
import math
from queue import PriorityQueue

class MySquareGrid:
    def __init__(self, arg):
        if type(arg) is int:
            self.numcells = arg * arg
            self.dim = arg
            self._g = [None for _ in range(self.numcells)]
        elif type(arg) is MySquareGrid:
            self.numcells = arg.numcells
            self.dim = arg.dim
            self._g = arg._g[:]
        else:
            raise TypeError("Argument should be int or MySquareGrid type.")
    
    def __getitem__(self, key):
        row = key[0]
        col = key[1]

        assert(row < self.dim and col < self.dim)

        return self._g[self.dim * row + col]

    def __setitem__(self, key, val):
        row = key[0]
        col = key[1]

        assert(row < self.dim and col < self.dim)
        self._g[self.dim * row + col] = val

    
    def __str__(self):
        s = ""
        for row in range(self.dim):
            s += "\n\n" if row > 0 else ""
            for col in range(self.dim):
                s += f"\t{self[row,col]}" if col > 0 else f"{self[row,col]}"
        return s


class SearchNode:
    def __init__(self, g_node=0, profit=0, visited=[], path=0,
                 capacity=None, bound=None):
        self.g_node = g_node
        self.profit = profit
        self.visited = visited
        self.path = path
        self.capacity = capacity
        self.bound = bound

    def __lt__(self, obj):
        if self.bound != obj.bound:
            return self.bound < obj.bound
        
        return self.profit < obj.profit

class GraphData:
    def __init__(self, g, prizes, capacity, start):
        self.graph = g
        self.num_nodes = g.dim
        self.prizes = prizes
        self.capacity = capacity
        self.start = start


#################
# Shortest Path #
#################

def find_shortest_paths(g, start):
    distances = [math.inf for _ in range(g.dim)]
    distances[start] = 0
    visited = [False for _ in range(g.dim)]
    paths = [[[]] for _ in range(g.dim)]
    current = None
    closest_node = None

    while not all(visited):
        closest_distance = math.inf
        for node, distance in enumerate(distances):
            if distance < closest_distance and not visited[node]:
                closest_node = node
                closest_distance = distance
        if math.isinf(closest_distance):
            break

        current = closest_node
        for dest in range(g.dim):
            if dest == current:
                continue
            if g[current, dest] + closest_distance < distances[dest]:
                distances[dest] = g[current, dest] + closest_distance
                for p in paths[current]:
                    paths[dest] = [p + [dest]]
            elif g[current, dest] + closest_distance == distances[dest]:
                for p in paths[current]:
                    paths[dest].append(p + [dest])
            
        visited[current] = True

    return paths, distances

###########
# BPC TSP #
###########

def make_pathlist_set(pl):
    preproc_pl = map(lambda x : [x] if type(x) is not list else x, pl)
    path_nodes = set()
    for arr in preproc_pl:
        for n in arr:
            path_nodes.add(n)

    return path_nodes
    

def find_bound(lengths, paths, cur_profit, prizes, visited, capacity):
    unvisited_lengths = [(dest, dist) for dest, dist in enumerate(lengths) if dest not in visited]
    unvisited_lengths.sort(key=(lambda x: x[1]), reverse=True)
    unvisited_prizes = [prizes[x[0]] for x in unvisited_lengths]
    total_unvisited = sum(unvisited_prizes) + cur_profit

    first_fit = None
    for idx, (dest, dist) in enumerate(unvisited_lengths):
        if dist > capacity:
            total_unvisited -= prizes[dest]
        elif dist == capacity:
            first_fit = idx
            break

    if not first_fit:
        return total_unvisited
    
    first_fit_paths_raw = paths[unvisited_lengths[first_fit][0]]
    first_fit_nodes = make_pathlist_set(first_fit_paths_raw)
    for i in range(first_fit + 1, len(unvisited_lengths)):
        cur_paths_raw = paths[unvisited_lengths[i][0]]
        cur_nodes = make_pathlist_set(cur_paths_raw)
        if first_fit_nodes.intersection(cur_nodes):
            continue
        elif unvisited_lengths[first_fit][1] + unvisited_lengths[i][1] > capacity:
            lesser_contributor = min(prizes[unvisited_lengths[i][0]],
                                     prizes[unvisited_lengths[first_fit][0]])
            total_unvisited -= lesser_contributor
            break
        else:
            break
    return total_unvisited
            
def init_find_best_path(gd):
    best_found = gd.prizes[gd.start]
    best_found_path = [gd.start]

    profit = gd.prizes[gd.start]

    current = gd.start

    visited = [gd.start]
    path = [gd.start]

    cap = gd.capacity

    shortest_paths, shortest_lengths = find_shortest_paths(gd.graph, current)
    upper_bound = find_bound(shortest_lengths, shortest_paths, profit, gd.prizes, visited, cap)

    return best_found, \
           best_found_path, \
           SearchNode(g_node=current, profit=profit, visited=visited, path=path, capacity=cap, bound=upper_bound), \
           shortest_paths, shortest_lengths


def build_new_search_node(gd, sn, destination, path, new_cap, shortest_paths, shortest_lengths, best_found):
    new_profit = sn.profit
    new_visited = sn.visited[:]
    for step in path:
        if step not in sn.visited:
            new_profit += gd.prizes[step]
            new_visited.append(step)
    new_path = sn.path + path
    new_upper_bound = find_bound(shortest_lengths, shortest_paths, new_profit, gd.prizes, new_visited, new_cap)
    if new_upper_bound > best_found:
        return SearchNode(g_node=destination, profit=new_profit, visited=new_visited, path=new_path, capacity=new_cap, bound=new_upper_bound)
    else:
        return None


def find_best_path(gd):
    q = PriorityQueue()
    best_found, best_found_path, first_search_node, sp, sl = init_find_best_path(gd)
    shortest_paths, shortest_lengths = sp, sl
    q.put((-first_search_node.bound, first_search_node))

    first_loop = True
    while not q.empty():
        sn = q.get()[-1]
        if sn.bound < best_found:
            break
        if first_loop:
            first_loop = False
        else:
            shortest_paths, shortest_lengths = find_shortest_paths(gd.graph, sn.g_node)
                
        for destination in range(gd.num_nodes):
            if destination in sn.visited or sn.capacity < shortest_lengths[destination]:
                continue

            new_cap = sn.capacity - shortest_lengths[destination]
            for path in shortest_paths[destination]:
                new_sn = build_new_search_node(gd=gd, sn=sn,
                                               destination=destination, path=path,
                                               new_cap=new_cap, shortest_paths=shortest_paths,
                                               shortest_lengths=shortest_lengths, best_found=best_found)
                
                if new_sn:
                    if new_sn.profit > best_found:
                        best_found, best_found_path = new_sn.profit, new_sn.path
                    q.put((-new_sn.bound, new_sn))
    
    return best_found_path, best_found

##################
# Graph Creation #
##################

def construct_graph(dim, unconnected, max_edge_weight, max_node_weight):
    g = MySquareGrid(dim)
    
    edge_sum = 0

    for row in range(dim):
        for col in range(dim):
            if row == col:
                g[row, col] = 0
            elif random.randrange(unconnected) == 0:
                g[row, col] = math.inf
            else:
                n = random.randrange(1, max_edge_weight)
                g[row, col] = n
                edge_sum += n
    
    prizes = [random.randrange(1, max_node_weight) for _ in range(dim)]

    return g, edge_sum, prizes



######
# IO #
######

def get_generation_params():
    dim = int(input("Enter the number of nodes => "))
    unconnected = int(input("Roughly 1 in x node pairs have no edges between them. Choose x => "))
    max_edge_weight = int(input("Choose maximum edge weight => "))
    max_node_weight = int(input("Choose maximum prize amount => "))

    return dim, unconnected, max_edge_weight, max_node_weight

def display_graph(g, edge_sum, prizes):
    print("Generated graph)\n")
    print(g)
    # 1print("\n\nPrizes)")
    prizes_str = ""
    for i, w in enumerate(prizes):
        prizes_str += f", {i}: {w}" if prizes_str else f"\nPrizes) {i}: {w}"
    print(prizes_str)
    print(f"\nSum of all edges) {edge_sum}\n")

def get_remaining_graph_data():
    capacity = int(input("Choose capacity => "))
    start = int(input("Choose starting node => "))
    return capacity, start

def display_results(path, profit):
    print("Path: " + ", ".join(map(str, path)))
    print(f"Profit: {profit}")

###########
# Testing #
###########

def sanity_check_result(g, prizes, capacity, best_path, best_profit):
    prize_sum = 0

    visited = []

    for node in best_path:
        if node not in visited:
            prize_sum += prizes[node]
            visited.append(node)
    
    path_sum = 0

    for i in range(len(best_path) - 1):
        path_sum += g[best_path[i], best_path[i + 1]]
    
    if prize_sum != best_profit:
        print("Uh oh! The sum of the prizes does not add up to best profit!")
    else:
        print("Prize total looks good!")
    
    if path_sum > capacity:
        print("Oh dear! The path exceeds the capacity!")
    else:
        print("Path length looks good!")

########
# Main #
########

def main():
    dim, unconnected, max_edge_weight, max_node_weight = get_generation_params()
    g, edge_sum, prizes = construct_graph(dim, unconnected, max_edge_weight, max_node_weight)
    display_graph(g, edge_sum, prizes)
    capacity, start = get_remaining_graph_data()
    data = GraphData(g, prizes, capacity, start)
    best_path, best_profit = find_best_path(data)
    # sanity_check_result(g, prizes, capacity, best_path, best_profit)
    display_results(best_path, best_profit)


main()
    
