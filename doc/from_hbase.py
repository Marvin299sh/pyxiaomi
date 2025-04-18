from hbase import HTable
def hbasepythonexample():
    table = HTable('test', '127.0.0.1', 9090)
    put = table.put('row1')
    put.add_column('cf', 'col1', 'value1')
    table.write()
    scan = table.scan()
    for row in scan:
        print(row)
        table.close()
        if name == 'main':
            hbasepythonexample()