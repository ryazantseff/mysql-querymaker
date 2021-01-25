import asyncio, logging, unittest
from query_maker import MySql, Table

class MyTest(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        # logging.basicConfig(level=logging.DEBUG)
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


if __name__ == '__main__':
    unittest.main()
