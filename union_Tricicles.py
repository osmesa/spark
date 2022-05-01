

import sys
from itertools import combinations
from pyspark import SparkContext


def getEdges(line):
    arguments = line.split(",")
    arg1 = arguments[0]
    arg2 = arguments[1]
    if arg1 < arg2:
        return arg1, arg2
    elif arg1 > arg2:
        return arg2, arg1
    else:
        None


def transformate(row):
    x, lista = row[0], list(row[1])
    result = []
    for y in lista:
        if x != y:
            el = (x, y), 'exists'
            if el not in result:
                result.append(el)
    for a, b in combinations(lista, 2):
        if a < b:
            el = (a, b), ('pending', x)
            if el not in result:
                result.append(el)
        elif a > b:
            el = (b, a), ('pending', x)
            if el not in result:
                result.append(el)
    return result


def thereIsExists(row):
    key, values = row
    for t in values:
        if t == "exists":
            return True
    else:
        return False


def findTricicles(row):
    key, values = row
    edges = []
    for t in values:
        if t != "exists":
            _, arg1 = t
            arg2, n3 = key
            edges.append((arg1, arg2, n3))
    return edges


def main():
    sc = SparkContext()

    rdd = sc.textFile(sys.argv[1])
    i = 2
    while 1 < i < len(sys.argv):
        x = sc.textFile(sys.argv[i])
        rdd = sc.union([x, rdd])
        i += 1

    rdd = rdd.map(getEdges).filter(lambda x: x != None).groupByKey()
    rdd = rdd.flatMap(transformate).groupByKey().filter(thereIsExists).flatMap(findTricicles)

    result = rdd.collect()

    print(f"{len(result)} triciclos: {result}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Triciclos_Union: Especifique al menos dos ficheros")
    else:
        main()
