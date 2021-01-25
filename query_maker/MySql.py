import aiomysql, logging
from .QueryMaker import QueryMaker 

class MySql:
    def __init__(self, host, user, db = None, pwd = '', con = None, dbName = 'Manager', loop=None):
        self.host = host
        self.user = user
        self.pw = pwd
        self.dbName = dbName
        self.db = db
        self.con = con
        self.loop = loop
        self.QueryMaker = QueryMaker(dbName, self.query)
        
      
    async def connect(self):
        logging.debug("db pass is {}".format(self.pw))
        pool = await aiomysql.create_pool(
            host = self.host,
            user = self.user,
            password = self.pw, 
            db = self.db,
            cursorclass = aiomysql.cursors.DictCursor,
            loop = self.loop
        )
        self.con = pool
        return pool

    async def query(self, q, _type = "select"):
        try:
            await self.connect()
            async def fetch(cursor, connectionInstance):
                return await cursor.fetchall()
            async def commit(cursor, connectionInstance):
                await connectionInstance.commit()
                return []
            typeAction = {
                'select': fetch,
                'insert': commit,
                'delete': commit,
                'update': commit,
                'truncate': commit,
                'create': commit
            }
            async with self.con.acquire() as conInst:
                async with conInst.cursor() as cur:
                    if isinstance(q, list):
                        for i in q:
                            await cur.execute(i)
                    else:
                        await cur.execute(q)
                    result = await typeAction[_type](cur, conInst)
            self.con.close()
            await self.con.wait_closed()    
            return result
        except Exception as e:
            logging.debug(str(e))
            return []

    async def getEverythingFrom(self, tableName):
        result = await self.query("SELECT * FROM {}.{}".format(self.dbName, tableName))
        return result
        
    async def putOneInto(self, tableName, fields, values):
        F = ', '.join(map(lambda i: "`{}`".format(i), fields))
        V = ', '.join(map(lambda i: "'{}'".format(i), values))
        Q = "INSERT INTO `{}`.`{}` ({}) VALUES ({});".format(self.dbName, tableName, F, V)
        logging.debug(Q)
        result = await self.query(Q, _type='insert')
        return result

    async def updateOneIn(self, tableName, field, value, conditions):
        C = ""
        for condition in conditions:
            if isinstance(condition, list):
                C = "{} {}{}'{}'".format(C, condition[0], condition[1], condition[2])
            else:
                C = "{} {}".format(C, condition)
        Q = "UPDATE {}.{} SET {}='{}' WHERE {}".format(self.dbName, tableName, field, value, C)
        logging.debug(Q)
        result = await self.query(Q, _type='update')
        return result

    async def createDbIfNotExists(self):
        Q = f"CREATE DATABASE IF NOT EXISTS {self.dbName}"
        logging.debug(Q)
        result = await self.query(Q, _type='create')
        return result    

    async def showDatabases(self):
        result = await self.query("SHOW DATABASES")
        return result 

    async def showTables(self):
        result = await self.query(f"SHOW TABLES FROM {self.dbName}")
        return result 

    async def describeTable(self, tableName):
        result = await self.query(f"DESCRIBE {self.dbName}.{tableName}")
        return result 

    async def createTableIfNotExists(self, name, config):
        Q = f"CREATE TABLE IF NOT EXISTS `{self.dbName}`.`{name}` ({', '.join(config)})" 
        logging.debug(Q)
        result = await self.query(Q, _type='create')
        return result 

    async def deleteOneFrom(self, tableName, conditions):
        C = ""
        for condition in conditions:
            if isinstance(condition, list):
                C = "{} {}{}'{}'".format(C, condition[0], condition[1], condition[2])
            else:
                C = "{} {}".format(C, condition)
        Q = "DELETE FROM {}.{} WHERE {}".format(self.dbName, tableName, C)
        logging.debug(Q)
        result = await self.query(Q, _type='delete')
        return result

    async def delOneFrom(self, tableName, field, value):
        Q = "DELETE FROM {}.{} WHERE {}='{}';".format(self.dbName, tableName, field, value)
        logging.debug(Q)
        result = await self.query(Q, _type='delete')
        return result

    async def getSimilarFrom(self, tableName, field, value, limit = None):
        Q = "SELECT * FROM {}.{} WHERE {} LIKE '%{}%' ORDER BY {}".format(self.dbName, tableName, field, value, field)
        if limit is not None:
             Q = "{} LIMIT {}".format(Q, limit)
        result = await self.query(Q)
        return result

    async def rawQuery(self, query, _type='select'):
        return await self.query(query, _type)