import logging

class QueryMaker:
    def __init__(self, dbName, queryFn):
        self.dbName = dbName
        self.type = 'select'
        self.query = queryFn
    
    def newQuery(self, config=None):
        if config is None:
            return Query(self.dbName, self.query)
        else:
            return Query(self.dbName, self.query, customConfig=config)

    def Select(self, fields):
        return Query(self.dbName, self.query).Select(fields)

    def Truncate(self, table):
        return Query(self.dbName, self.query).Truncate(table)

    def Insert(self, table, fields):
        return Query(self.dbName, self.query).Insert(table, fields)

    def InsertIgnore(self, table, fields):
        return Query(self.dbName, self.query).InsertIgnore(table, fields)

    def Delete(self, fromTable=None):
        return Query(self.dbName, self.query).Delete(fromTable)

    def Update(self, table):
        return Query(self.dbName, self.query).Update(table)

    def Config(self, conf):
        return Query(self.dbName, self.query, customConfig=conf)

class Query:
    def __init__(self, dbName, queryFn, customConfig={}):
        self.dbName = dbName
        self.Q = ""
        self.type = 'select'
        self.query = queryFn
        self.customConfig = customConfig

    def parseAlias(self, i):
        if isinstance(i, list):
            return ' AS '.join(i)
        else:
            return i
    
    def addDbName(self, i):
        if self.customConfig.get('dataBasePrefix'):
            return "{}.{}".format(self.dbName, i)
        else:
            return i

    def Truncate(self, table):
        self.type = 'truncate'
        self.Q = "TRUNCATE TABLE {}.{}".format(self.dbName, table)
        return self

    def Update(self, table):
        self.type = 'update'
        self.Q = "UPDATE {}.{}".format(self.dbName, table)
        return self

    def Set(self, vals):
        def parseVal(val):
            return f"{val[0]} = '{val[1]}'"
        newVals = ", ".join(map(parseVal, vals))
        self.Q = f"{self.Q} SET {newVals}"
        return self

    def Insert(self, table, fields):
        self.type = 'insert'
        F = "({})".format(', '.join(fields))
        self.Q = "INSERT INTO {}.{} {}".format(self.dbName, table, F)
        return self

    def InsertIgnore(self, table, fields):
        self.type = 'insert'
        F = "({})".format(', '.join(fields))
        self.Q = "INSERT IGNORE INTO {}.{} {}".format(self.dbName, table, F)
        return self

    def Values(self, vals):
        R = []
        for r in vals:
            R.append("({})".format(', '.join(map(lambda i: "'{}'".format(i), r))))
        self.Q = "{} VALUES {}".format(self.Q, ', '.join(R))
        return self

    def Select(self, fields):
        if isinstance(fields, list):
            F = ', '.join(map(self.parseAlias, fields))
        else: 
            F = fields
        if self.Q == "":    
            self.type = 'select'
            self.Q = "SELECT {}".format(F)
        else:
            self.Q = "{} SELECT {}".format(self.Q, F)
        return self

    def Delete(self, fromTable=None):
        F = ""
        if fromTable:
            if isinstance(fromTable, list):
                F = ' {}'.format(', '.join(map(self.addDbName, fromTable)))
            else:
                F = ' {}'.format(self.addDbName(fromTable))
        self.type = 'delete'
        self.Q = "DELETE{}".format(F)
        return self

    def From(self, tableName, multiple=False):
        def parseTblName(tableName):
            if isinstance(tableName, list):
                return self.parseAlias(tableName)
            else: 
                return tableName

        if multiple:
            TN = ', '.join(
                map(
                    lambda i: "{}.{}".format(self.dbName, i),
                    map(parseTblName, tableName)
                )
            )
            self.Q = "{} FROM {}".format(self.Q, TN)
        else:
            # if isinstance(tableName, list):
            #     TN = self.parseAlias(tableName)
            # else: 
            #     TN = tableName
            TN = parseTblName(tableName)
            self.Q = "{} FROM {}.{}".format(self.Q, self.dbName, TN)
        return self

    def InnerJoin(self, tableName):
        if isinstance(tableName, list):
            TN = self.parseAlias(tableName)
        else: 
            TN = tableName
        self.Q = "{} INNER JOIN {}.{}".format(self.Q, self.dbName, TN)
        return self

    def LeftJoin(self, tableName):
        if isinstance(tableName, list):
            TN = self.parseAlias(tableName)
        else: 
            TN = tableName
        self.Q = "{} LEFT JOIN {}.{}".format(self.Q, self.dbName, TN)
        return self

    def RightJoin(self, tableName):
        if isinstance(tableName, list):
            TN = self.parseAlias(tableName)
        else: 
            TN = tableName
        self.Q = f"{self.Q} RIGHT JOIN {self.dbName}.{TN}"
        return self

    def CrossJoin(self, tableName):
        if isinstance(tableName, list):
            TN = self.parseAlias(tableName)
        else: 
            TN = tableName
        self.Q = "{} CROSS JOIN {}.{}".format(self.Q, self.dbName, TN)
        return self

    def On(self, joinCond):
        self.Q = "{} ON {}={}".format(self.Q, self.addDbName(joinCond[0]), self.addDbName(joinCond[1]))
        return self

    def Where(self, conditions):
        def parseCond(condition):
            if isinstance(condition, list):
                if not condition[2]:
                    return "{} IS NULL".format(self.addDbName(condition[0]))
                else:
                    return "{} {} '{}'".format(self.addDbName(condition[0]), condition[1], condition[2])
            else:
                return condition
        C = " ".join(map(parseCond, conditions))
        self.Q = '{} WHERE {}'.format(self.Q, C)
        return self

    def WhereExists(self, sub):
        self.Q = '{} WHERE EXISTS {}'.format(self.Q, sub)
        return self

    def Sub(self):
        return "({})".format(self.Q)
    
    def OrderBy(self, field, orderType='ASC'):
        self.Q = "{} ORDER BY {} {}".format(self.Q, field, orderType)
        return self

    def Limit(self, amount):
        self.Q = "{} LIMIT {}".format(self.Q, amount)
        return self

    async def Run(self, debug=False):
        if debug:
            logging.debug(self.Q)
        return await self.query(self.Q, _type=self.type)
    
