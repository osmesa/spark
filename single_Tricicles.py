

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


def main(fname):
    sc = SparkContext()
    rdd = sc.textFile(fname)

    rdd = rdd.map(getEdges).filter(lambda x: x != None).groupByKey()
    rdd = rdd.flatMap(transformate).groupByKey().filter(thereIsExists).flatMap(findTricicles)

    result = rdd.collect()

    print(f"{len(result)} Triciclos: {result}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Triciclos_Unico: Especifique un único fichero")
    else:
        main(sys.argv[1])
