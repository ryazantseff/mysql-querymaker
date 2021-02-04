import asyncio, logging, unittest
from query_maker import MySql, Table
from .alchemy import Alchemy

class MyTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        loop = asyncio.get_event_loop()
        self.db = MySql(
            'db',
            'root',
            '',
            dbName='Test',
            loop=loop
        )

    async def test_create_db(self):
        await self.db.createDbIfNotExists()
        self.assertTrue(Table(await self.db.showDatabases()).contains('Database', 'Test'))

    async def test_create_table(self):
        await self.db.createTableIfNotExists('TestTable', [
            "id int(11) NOT NULL AUTO_INCREMENT",
            "uid int(11) DEFAULT NULL",
            "gid int(11) NOT NULL DEFAULT -1",
            "name varchar(100) NOT NULL",
            "pass varchar(500) DEFAULT NULL",
            "local tinyint(1) DEFAULT 0",
            "ad tinyint(1) DEFAULT 0",
            "PRIMARY KEY (id)"
        ])
        self.assertTrue(Table(await self.db.showTables()).contains('Tables_in_Test', 'TestTable'))
        # Table(await self.db.describeTable('TestTable')).log()
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'id') ==
            {'Field': 'id', 'Type': 'int(11)', 'Null': 'NO', 'Key': 'PRI', 'Default': None, 'Extra': 'auto_increment'}
        )
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'uid') ==
            {'Field': 'uid', 'Type': 'int(11)', 'Null': 'YES', 'Key': '', 'Default': None, 'Extra': ''}
        )
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'gid') ==
            {'Field': 'gid', 'Type': 'int(11)', 'Null': 'NO', 'Key': '', 'Default': '-1', 'Extra': ''}
        )
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'pass') ==
            {'Field': 'pass', 'Type': 'varchar(500)', 'Null': 'YES', 'Key': '', 'Default': None, 'Extra': ''}
        )
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'local') ==
            {'Field': 'local', 'Type': 'tinyint(1)', 'Null': 'YES', 'Key': '', 'Default': '0', 'Extra': ''}
        )
        self.assertTrue(
            Table(await self.db.describeTable('TestTable')).findRow('Field', 'ad') ==
            {'Field': 'ad', 'Type': 'tinyint(1)', 'Null': 'YES', 'Key': '', 'Default': '0', 'Extra': ''}
        )

    async def test_insertBlob(self):
        await self.db.createTableIfNotExists('blobInsertTable', [
            "id int(11) NOT NULL AUTO_INCREMENT",
            "name varchar(100) NOT NULL",
            "image BLOB DEFAULT NULL",
            "PRIMARY KEY (id)"
        ])
        self.assertTrue(Table(await self.db.showTables()).contains('Tables_in_Test', 'blobInsertTable'))
        # (
        #     await self.db.QueryMaker
        #         .Insert('blobInsertTable', ['name', 'image'])
        #         .Values([['testName', b'\xd0\x91\xd0\xb0\xd0\xb9\xd1\x82\xd1\x8b']])
        #         .Run(debug=True)
        # )
        await self.db.query("INSERT INTO blobInsertTabe (name, image) VALUES (%s)", args = (
            ('testName2', b'\xd0\x91\xd0\xb0\xd0\xb9\xd1\x82\xd1\x8b'),
        ), _type='insert')
        self.assertEqual(
            Table(await self.db.getEverythingFrom('blobInsertTable')).findRow('name', 'testName'),
            {'name': 'testName', 'image': b'\xd0\x91\xd0\xb0\xd0\xb9\xd1\x82\xd1\x8b'}
        )

if __name__ == '__main__':
    unittest.main()
