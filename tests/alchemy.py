import asyncio, logging, unittest
import sqlalchemy as sa
import pandas as pd
from aiomysql.sa import create_engine
from query_maker import MySql, Table

class Alchemy (unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        logging.basicConfig(level=logging.DEBUG)
        loop = asyncio.get_event_loop()
        self.db = MySql(
            host = 'db',
            user = 'root',
            pwd = '',
            dbName='saTest',
            loop=loop
        )
        await self.db.createDbIfNotExists()
        

    async def test_deleteSelect(self):
        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `saTable4` (
            `id` serial PRIMARY KEY,
            `val` varchar(255)
        )''')

        metadata = sa.MetaData()

        tbl = sa.Table('saTable4', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('val', sa.String(255))
        )
        
        res = await self.db.SaQuery([
            tbl.insert().values([{'val':'testToDel'}, {'val':'testToDel2'}]),
            sa.select([tbl])
        ], echo=True)

        df = pd.DataFrame(res[0])
        self.assertTrue('testToDel' in df['val'].values)
        self.assertTrue('testToDel2' in df['val'].values)

        res = await self.db.SaQuery([
            tbl.delete().where(tbl.c['val'] == 'testToDel'),
            sa.select([tbl])
        ], echo=True)
        logging.debug(res)



    async def test_insertSelect(self):
        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `saTable3` (
            `id` serial PRIMARY KEY,
            `val` varchar(255)
        )''')

        metadata = sa.MetaData()

        tbl = sa.Table('saTable3', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('val', sa.String(255))
        )
        
        res = await self.db.SaQuery([
            tbl.insert().values([{'val':'testToDel'}, {'val':'testToDel2'}]),
            sa.select([tbl])
        ], echo=True)

        # logging.debug(res)
        df = pd.DataFrame(res[0])
        self.assertTrue('testToDel' in df['val'].values)
        self.assertTrue('testToDel2' in df['val'].values)


    async def test_delete(self):
        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `saTable2` (
            `id` serial PRIMARY KEY,
            `val` varchar(255)
        )''')

        metadata = sa.MetaData()

        tbl = sa.Table('saTable2', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('val', sa.String(255))
        )
        
        await self.db.SaQuery([
            tbl.insert().values([{'val':'testToDel'}, {'val':'testToDel2'}])
        ], echo=True)

        df = pd.DataFrame(await self.db.SaQuery(
            sa.select([tbl]), echo=True
        ))
        self.assertTrue('testToDel' in df['val'].values)
        self.assertTrue('testToDel2' in df['val'].values)

        await self.db.SaQuery(
            tbl.delete().where(tbl.c['val'] == 'testToDel')
        )
        df = pd.DataFrame(await self.db.SaQuery(
            sa.select([tbl])
        ))

        self.assertFalse('testToDel' in df['val'].values)
        self.assertTrue('testToDel2' in df['val'].values)


    async def test_create_db(self):
        self.assertTrue(Table(await self.db.showDatabases()).contains('Database', 'saTest'))

    async def test_SA(self):
        metadata = sa.MetaData()

        tbl = sa.Table('saTable', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('val', sa.String(255)),
            sa.Column('bin', sa.LargeBinary))

        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `saTable` (
            `id` serial PRIMARY KEY,
            `val` varchar(255),
            `bin` blob
        )''')

       
        await self.db.SaQuery(
            tbl.insert().values(val='aghsdgfhjadgs', bin = b'\xd0\x91\xd0\xb0\xd0\xb9\xd1\x82\xd1\x8b'),
            echo=True
        )

        await self.db.SaQuery([
            tbl.insert().values(val='abc'),
            tbl.insert().values(val='abasdfsdf'),
            tbl.insert().values(val='abqewrwerqewrc')
        ], echo=True)

        for row in (await self.db.SaQuery(tbl.select())):
            logging.debug(row.bin)

        for i in (await self.db.SaQuery([
            sa.select([tbl]).where(tbl.c.val == 'abc'),
            sa.select([tbl]).where(tbl.c.val == 'abasdfsdf'),
            
        ])):
            Table(i).log()

        logging.debug(list(map(lambda i: i[0], await self.db.SaQuery(sa.select([tbl.c.val])))))

        await self.db.SaQuery('TRUNCATE TABLE saTable')

        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `users` (
            `id` serial PRIMARY KEY,
            `name` varchar(255) UNIQUE
        )''')

        users = sa.Table('users', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.String(255)))

        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `groups` (
            `id` serial PRIMARY KEY,
            `name` varchar(255) UNIQUE
        )''')

        groups = sa.Table('groups', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('name', sa.String(255)))

        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `usersInGroup` (
            `userId` bigint unsigned not null,
            `groupId` bigint unsigned not null,
            UNIQUE(`userId`, `groupId`)
        )''')

        usersInGroup = sa.Table('usersInGroup', metadata,
            sa.Column('userId', sa.Integer),
            sa.Column('groupId', sa.Integer))

        await self.db.SaQuery([
            users.insert().values(name='User1'),
            users.insert().values(name='User2'),
            users.insert().values(name='User3'),
            users.insert().values(name='User4'),
            groups.insert().values(name='Group1'),
            groups.insert().values(name='Group2')
        ], echo=True)

        await self.db.SaQuery([
            usersInGroup.insert().from_select([usersInGroup.c.userId, usersInGroup.c.groupId], 
                sa.select([users.c.id, groups.c.id], use_labels=True).where(
                    sa.and_(users.c.name.in_(['User2', 'User1']) , groups.c.name == 'Group1'))
            ),
            usersInGroup.insert().from_select([usersInGroup.c.userId, usersInGroup.c.groupId], 
                sa.select([users.c.id, groups.c.id], use_labels=True).where(
                    sa.and_(users.c.name.in_(['User4', 'User3', 'User2']) , groups.c.name == 'Group2'))
            )
        ])

        sel1 = sa.select([users.c.name, groups.c.name], use_labels=True).where(sa.and_(
            usersInGroup.c.userId == users.c.id,
            usersInGroup.c.groupId == groups.c.id,
            groups.c.name == 'Group1'
        ))
        
        sel2 = sa.select([users.c.name, groups.c.name], use_labels=True).where(sa.and_(
            usersInGroup.c.userId == users.c.id,
            usersInGroup.c.groupId == groups.c.id,
            groups.c.name == 'Group2'
        ))

        gr1 = pd.DataFrame(await self.db.SaQuery(sel1, echo=True))
        gr2 = pd.DataFrame(await self.db.SaQuery(sel2, echo=True))

        logging.warning(f'\n{gr1.to_string()}')
        logging.warning(f'\n{gr2.to_string()}')

    async def test_updateEntry(self):
        await self.db.SaQuery('''CREATE TABLE IF NOT EXISTS `updateTest` (
            `id` serial PRIMARY KEY,
            `val` varchar(255) UNIQUE
        )''')

        metadata = sa.MetaData()

        tbl = sa.Table('updateTest', metadata,
            sa.Column('id', sa.Integer, primary_key=True),
            sa.Column('val', sa.String(255))
        )
        
        resId = await self.db.SaQuery([
            tbl.insert().values([{'val':'goingToUpdate'}]),
            sa.select([tbl.c.id]).where(tbl.c.val == 'goingToUpdate')
        ], echo=True)

        res = await self.db.SaQuery([
            tbl.update().where(tbl.c.val == 'goingToUpdate').values(val='updated'),
            sa.select([tbl])
        ], echo=True)

        # logging.debug(res[0][0])
        self.assertTrue(res[0][0][1] == 'updated')
        # df = pd.DataFrame(res[0])
        # self.assertTrue('testToDel2' in df['val'].values)