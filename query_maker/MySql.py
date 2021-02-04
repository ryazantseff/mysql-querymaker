import aiomysql, logging
import sqlalchemy as sa
from aiomysql.sa import create_engine
from .QueryMaker import QueryMaker 

class MySql:
    def __init__(self, host = '', user = '', pwd = '', dbName = '', loop=None):
        self.host = host
        self.user = user
        self.pw = pwd
        self.dbName = dbName
        self.loop = loop
        self.QueryMaker = QueryMaker(dbName, self.query)
        
    async def SaEngine(self):
        self.engine = await create_engine(
            user=self.user,
            db=self.dbName,
            host=self.host,
            password=self.pw,
            loop=self.loop,
            echo = True
        )
        return self.engine

    async def SaQuery(self, query):
        try:
            await self.SaEngine()
            async with self.engine.acquire() as conn:
                if isinstance(query, list):
                    result = []
                    for i in query:
                        if isinstance(i, sa.sql.selectable.Select):
                            result.append(await (await conn.execute(i)).fetchall())
                        elif isinstance(i, sa.sql.dml.Insert):
                            await conn.execute(i)
                        elif isinstance(i, str):
                            await conn.execute(i)
                    await conn.execute("commit")
                    return result
                elif isinstance(query, sa.sql.dml.Insert):
                    await conn.execute(query)
                    await conn.execute("commit")
                elif isinstance(query, str):
                    await conn.execute(query)
                    await conn.execute("commit")
                elif isinstance(query, sa.sql.selectable.Select):
                    return await (await conn.execute(query)).fetchall()
        except Exception as e:
            logging.debug(str(e))
        finally:
            if hasattr(self, 'engine') and self.engine is not None:
                self.engine.close()
                await self.engine.wait_closed()
    

    async def connect(self, _type):
        logging.debug(f"db pass is {self.pw}")
        pool = await aiomysql.create_pool(
            host = self.host,
            user = self.user,
            password = self.pw, 
            db = None if _type == 'create' else self.dbName,
            cursorclass = aiomysql.cursors.DictCursor,
            loop = self.loop
        )
        self.con = pool
        return pool

    def setDb(self, dbName):
        self.dbName = dbName

    async def query(self, q, _type = "select", args = None):
        try:
            await self.connect(_type)
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
                    if args is not None:
                        await cur.executemany(q, args)
                    else:
                        if isinstance(q, list):
                            for i in q:
                                await cur.execute(i)
                        else:
                            await cur.execute(q)
                result = await typeAction[_type](cur, conInst)
            return result
        except Exception as e:
            logging.debug(str(e))
            return []
        finally:
            if hasattr(self, 'con') and self.con is not None:
                self.con.close()
                await self.con.wait_closed()    

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