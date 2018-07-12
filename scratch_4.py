

class nodeA( object ):
    name = 'NodeA'
    type = 'WF'
    needs = set( ['a'] )
    adds = set( ['b'] )

class nodeB( object ):
    name = 'NodeB'
    type = 'WF'
    needs = set( ['b'] )
    adds = set([ 'c', 'd' ])

class nodeC( object ):
    name = 'NodeC'
    type = 'WF'
    needs = set( ['b'] )
    adds = set( ['e'] )

class nodeD( object ):
    name = 'NodeD'
    type = 'AGG'
    needs = set( ['c', 'd', 'e'] )
    adds = set( ['c', 'd', 'e' ] )

class nodeE( object ):
    name = 'NodeE'
    type = 'WF'
    needs = set(['c', 'e'])
    adds = set(['f'])

engineList = [
    nodeA(), nodeB(), nodeC(), nodeD(), nodeE()
]

dependencyMap = {}
# Get all the dependent engines
for engine in engineList:
    dependencyMap[engine.name] = [ eng for eng in engineList if eng.adds.intersection( engine.needs ) and eng != engine ]
# If an aggregation forms part of the dependent engines then drop all other dependencies
# Assumption - working with the aggregated data
for k, v in dependencyMap.items():
    if len(v) > 1:
        for i in v:
            if i.type == 'AGG': dependencyMap[k] = [i]

# Nice human readable format
dependencyMap = { k: [v.name for v in dependencyMap[k]] for k, v in dependencyMap.items()}

print( dependencyMap )