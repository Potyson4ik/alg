import random
import multiprocessing as mp
from time import time
from pprint import pprint


def prim(G):
    n = len(G)

    U = set([0])
    F = list()
    W = set([i for i in range(1, n)])

    while len(W) != 0:
        min_weight = None
        for u in U:
            for v in W:
                if min_weight is None or min_weight > G[u][v]:
                    min_weight = G[u][v]
                    min_edge = (u, v)
        F.append(min_edge)
        U.add(min_edge[1])
        W.remove(min_edge[1])
    return F


def find_min_edge(q_in, q_out, G, W):
    while True:
        if not q_in.empty():
            data = q_in.get()
            if data is None:
                break
            calc_d, U = data[0], data[1]
            W = W - set(U)
            min_weight = None
            min_edge = None
            for u in U[calc_d[0]:calc_d[1]]:
                for v in W:
                    if min_weight is None or min_weight > G[u][v]:
                        min_weight = G[u][v]
                        min_edge = (u, v)
            q_out.put(min_edge)


def parallel_prim(G, p):
    n = len(G)

    U = [0]
    F = list()
    W = set([i for i in range(1, n)])

    q_in = mp.Queue()
    q_out = mp.Queue()

    p_list = []
    for i in range(p):
        pr = mp.Process(target=find_min_edge, args=(q_in, q_out, G, W))
        pr.start()
        p_list.append(pr)

    while len(W) > 0:
        min_weight = None
        step = max(len(U) // p, p)
        counter = 0
        for i in range(p):
            calc_d = (i*step, (i*step + step)) if i < (p - 1) else (i*step, i*step+len(U))
            if calc_d[0] > len(U):
                break
            q_in.put((calc_d, U))
            counter += 1
        while counter !=0:
            if not q_out.empty():
                e = q_out.get()
                if e is not None and (min_weight is None or min_weight > G[e[0]][e[1]]):
                    min_weight = G[e[0]][e[1]]
                    min_edge = e
                counter -= 1
        F.append(min_edge)
        U.append(min_edge[1])
        W.remove(min_edge[1])
    # Освобождение процессов
    for i in range(p):
        q_in.put(None)
    for pr in p_list:
        pr.join()
    return F


if __name__ == '__main__':
    # n = int(input('V count: '))

    t_result = [[] for i in range(4)]
    n_list = [100, 500, 1000, 2500, 5000]

    for n in n_list:
        G = [[0 for j in range(n)] for i in range(n)]

        for i in range(n):
            for j in range(i):
                G[i][j] = random.randint(1, n)
                G[j][i] = G[i][j]
        print('|V| = ', n)
        timer = time()
        alg1 = prim(G)
        t_result[0].append(time() - timer)

        timer = time()
        alg2 = parallel_prim(G, 2)
        t_result[1].append(time() - timer)

        timer = time()
        alg3 = parallel_prim(G, 4)
        t_result[2].append(time() - timer)

        timer = time()
        alg4 = parallel_prim(G, 8)
        t_result[3].append(time() - timer)

        pprint(t_result)
        # sum1 = sum([G[i][j] for i,j in alg1])
        # sum2 = sum([G[i][j] for i,j in alg2])
        # print(sum1 == sum2)