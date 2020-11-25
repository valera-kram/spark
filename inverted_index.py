import os
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("spark").setMaster("local")
sc = SparkContext

def find_files(root_dir: str):
    all_files = []
    for root, _, files in os.walk(root_dir):
        all_files += map(lambda s: root + '/' + s, files)
    return all_files

def split_set(c: str):
    return lambda p: [(p[0], s) for s in p[1].split(c)]

def replace_corn(w: str):
    i = 0
    while i < len(w) and w[i] in ".,:;\"\'/[](){}-_":
        i+=1
    j = len(w) - 1
    while j>=0 and w[j] in ".,:;\"\'/[](){}-_":
        j -=1
    return w[i:j+1]


rdd = sc.parallelize(map(lambda f: (f.split('/')[-1], str(open(f, 'rb').read())), find_files('/home/valera/Downloads/news20')))
rdd2 = rdd.flatMap(split_set('\n')).flatMap(split_set(' ')).flatMap(split_set(',').flatMap('\\'))
rdd3 = rdd2.map(replace_corn).filter(lambda p: len(p) == 2 and len(p[1]) != 0)
rdd4 = rdd3.filter(lambda p: all(map(str.isalpha, p[1]))).map(lambda p: (p[0], p[1].lower()))
rdd5 = rdd4.map(lambda p: (p[1], (1, {p[0]})))
rdd6 = rdd5.reduceByKey(lambda p, q: (p[0]+q[0], p[1].union(q[1]))).sortByKey()
f = open('index.csv', 'w')
f.write('\n'.join(map(lambda p: p[0] + ', ' + str(p[1][0] + ', ' + ' '.join(map(str, sorted(p[1][1]))), rdd6.collect()))))



# find_files('/home/valera/Downloads/news20')
# print(find_files('/home/valera/Downloads/news20'))