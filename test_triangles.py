def triangles(g, total_count):
    n = len(g)
    for a in range(0, n):
        for b in range(a + 1, n):
            if not g[a][b]:
                continue
            for c in range(b + 1, n):
                if g[b][c] and g[a][c]:
                    total_count += 1
    return total_count

import time
start_time = time.time()

file = open('graph.txt')
number_of_vertices = 0
for line in file:
    tmp_numbers = line.split()
    if int(tmp_numbers[0]) > number_of_vertices:
        number_of_vertices = int(tmp_numbers[0])
    if int(tmp_numbers[1]) > number_of_vertices:
        number_of_vertices = int(tmp_numbers[1])
file.close()

arr = [[0] * number_of_vertices for i in range(number_of_vertices)]

file = open('graph.txt')
for line in file:
    tmp_numbers = [int(n) for n in line.split()]
    arr[tmp_numbers[0]-1][tmp_numbers[1]-1] = 1
    arr[tmp_numbers[1]-1][tmp_numbers[0]-1] = 1
file.close()

total_count = triangles(arr, 0)
print(total_count)
print("--- %s seconds ---" % (time.time() - start_time)) 