import logging

class Table:
    def __init__(self, table):
        self.table = table

    def __iter__(self):
        return self.table.__iter__()

    def __next__(self):
        try:
            return self.table.__next__()
        except StopIteration:
            raise

    def __len__(self):
        l = 0
        for i in self.table:
            l += 1
        return l        

    def findRow(self, columnName, value):
        for i in self.table:
            if i[columnName] == value:
                return i
        return None

    def contains(self, columnName, value):
        if self.findRow(columnName, value) is not None:
            return True
        else:
            return False

    def map(self, config, filterFn=None, outType='list'):
        res = []
        filtered = self.table
        if filterFn is not None:
            filtered = list(filter(filterFn, self.table))
        for i in filtered:
            if isinstance(config, list):
                row = {
                    'list': [],
                    'dict': {}
                }[outType]
                for j in config:
                    if outType == 'dict':
                        row[j] = i[j]
                    else:
                        row.append(i[j])
            else:
                row = i[config]
            res.append(row)
        return res
 
    def log(self):
        for i in self.table:
            logging.debug(i)

    def isEmpty(self):
        if len(self.table) == 0:
            return True
        else:
            return False