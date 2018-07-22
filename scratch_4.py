import multiprocessing as mp
import time
import logging

class nodeA( object ):
    name = 'NodeA'
    type = 'WF'
    needs = set( ['a'] )
    adds = set( ['b'] )
    def run(self):
        print('Running NodeA')
        time.sleep(2)
        return self

class nodeB( object ):
    name = 'NodeB'
    type = 'WF'
    needs = set( ['b'] )
    adds = set([ 'c', 'd' ])
    def run(self):
        print('Running NodeB')
        time.sleep(2)
        return self

class nodeC( object ):
    name = 'NodeC'
    type = 'WF'
    needs = set( ['b'] )
    adds = set( ['e'] )
    def run(self):
        print('Running NodeC')
        time.sleep(2)
        return self

class nodeD( object ):
    name = 'NodeD'
    type = 'AGG'
    needs = set( ['c', 'd', 'e'] )
    adds = set( ['c', 'd', 'e' ] )
    def run(self):
        time.sleep(2)
        return self

class nodeE( object ):
    name = 'NodeE'
    type = 'WF'
    needs = set(['c', 'e'])
    adds = set(['f'])
    def run(self):
        time.sleep(2)
        return self


def getDependencyMap():
    engineList = [
        nodeA(), nodeB(), nodeC(), nodeD(), nodeE()
    ]

    dependencyMap = {}
    # Get all the dependent engines
    for engine in engineList:
        dependencyMap[engine] = [ eng for eng in engineList if eng.adds.intersection( engine.needs ) and eng != engine ]
    # If an aggregation forms part of the dependent engines then drop all other dependencies
    # Assumption - working with the aggregated data
    for k, v in dependencyMap.items():
        if len(v) > 1:
            for i in v:
                if i.type == 'AGG': dependencyMap[k] = [i]

    # Nice human readable format
    readableDependencyMap = { k.name: [v.name for v in dependencyMap[k]] for k, v in dependencyMap.items()}

    print( readableDependencyMap )

    for k in dependencyMap.keys():
        for v in dependencyMap[k]:
            print('(%s, OUT, %s, IN),' % (v.name, k.name) )
    return dependencyMap


# while len( dependencyMap ) > 0:
#     joblist = []
#     for key, val in list(dependencyMap.items()):
#         if len(val) == 0:
#             joblist.append( key )
#             del dependencyMap[key]
#             for k, v in list(dependencyMap.items()):
#                 if key in dependencyMap[k]: dependencyMap[k].remove(key)
#     print('%s jobs to run' % (len(joblist)))
#
#
# print( [x.name for x in joblist] )

def JobTracker(result, depMap):
    print('running callback')
    depMap = { k:v for k,v in depMap.items() if k.name != result.name }
    for k, v in list(depMap.items()):
        if result.name in [v.name for v in depMap[k]]:
            depMap[k] = [ v for v in depMap[k] if v.name != result.name ]
    return depMap

def consumer(objInstance):
    return objInstance.run()

if __name__ == '__main__':
    depMap = getDependencyMap()
    pool = mp.Pool()
    tasks = depMap.keys()
    tasks = [k for k in depMap.keys()]
    results = []
    while len(tasks) != 0:
        for task in tasks:
            if len(depMap[task]) == 0:
                print('running %s' % task)
                results.append( pool.apply_async(consumer, args=(task,)) )
                tasks.remove(task)
        for result in results:
            if result.ready():
                depMap = JobTracker(result.get(), depMap)
                results.remove(result)
                print( depMap )
