from models import Table, Column

###
# TBLUTILS UNIT TESTS
###
# TABLE TESTS
#	 LOADING DATA
#	 	* __init__
#	 	* load
#	 		- _read_csv
#			- _read_json
#			- _load_lists
#			- _load_dicts
#	 		- _load_columns

#	 SAVING DATA
#		* save
#			- _write_csv
#	 		- _write_json

#	 ACCESSING AND DISPLAYING DATA
#	 	* data
#	 	* select

#	 MODIFYING DATA
#	 	* filter
#		* sort
#	 	* desc
#		* add
#	 	* append
#	 	* insert

#	 DATABASES
#		* conn
#		* make_table
#	 	* write_to_table
#		* write_db
#	 	* read_db


# COLUMN TESTS
#	LOADING DATA
#		* __init__

# 	COMPARATORS

df = Table('student_data.csv')
df1 = df.sort('Cohort', 'First Name').select('Cohort', 'First Name').desc()
print(df1.data())

#print(Table())

#x = Column([1,5,9,15])
#y = Column([1,2,3,4])
#print(x)
#print(y)
#df = Table([x,y])
#print(df.select("Column1"))
#df.insert(20, z)
#print(df.data('list'))
#val = 4
#print(x._binary_search(val))
#x.insert(x._binary_search(val), val)
#print(x)


#df1 = df1.filter(df1.select('First Name') < 'Tom')
#df1 = df1.select('Cohort','First Name')
#print(df1.data(), df1)
#print(df1._write_create_table('student', 'First Name'))

#df1.open_connection(host='localhost', user='grayden', passwd='103lacrosse', db='csv_utilities', port=5432)
#df.write_to_db(table_name='customers', col_names=['name'])
#df1.make_db_table('student')
#print(df1.data())
#df1.write_to_db('student')
#df1.cur.execute('select * from student;')
#print(df1.cur.fetchall())

#for col in df1._data:
	#print(col._get_data_type())

#col = Column(['2019',1,2,3,4], 'numbers')
#col = Column(['1.0',1,2,3,4], 'numbers')
#x = df1.select('First Name')._get_data_type()
#x = col._get_data_type()
#print(x)



"""
# Filter ~ Multi-column
d1 = [name in ('Grayden', 'Tom') for name in df.select('First Name')]
print(df.filter(d1, df.select('Email') != '').select('First Name').data())
"""

# Column
"""
# Binary insertion sort
x = Column([1,6,7,8,10,5,4,3,3,9])
y = x.sort()
print(y._data, x._data)
#print(x._binary_search(2))

#for val in x:
	#print('val', val, 'idx', x._binary_search(val))
"""
"""
# _load_lists method
data = [['col1','col2'],['val1',0],['val2',1]]
df = Table(data)
print(df.data())
"""

"""
# _load_dicts method
data = [{'col1':'val1', 'col2':0},{'col1':'val2','col2':1}]
df = Table(data)
print(df.data())
"""

"""
# select method
subset = df.select('First Name', 'Session', 'Phone')
print(subset)
"""

"""
# filter method
rows = range(3)
subset = df.filter(rows=rows)
print(subset._data)

subset = df.filter(filters = {'First Name':{'Grayden','Caroline'}})
print(subset._data)
"""

"""
# select --> filter method chain
rows = range(3)
subset = df.select('Session', 'First Name', 'Phone').filter(rows=rows)
print(subset._data)
"""

"""
# save method
subset.save('student_data_copy.csv')
"""

"""
# sort method
sorted_df = df.sort(['Country'], True)
sorted_df.save('student_data_sorted.csv')
"""


# connect method
#df = df.select('First Name').filter()
#df.open_connection(host='localhost', user='grayden', passwd='103lacrosse', db='csv_utilities', port=5432)
#df.write_to_db(table_name='customers', col_names=['name'])
#df.cur.execute('select * from customers;')
#print(df.cur.fetchall())



# write_to_table method
#subset = df.select()
#subset.write_to_table()


"""
# add method
data = [['col1','col2'],['val1',0],['val2',1]]
df = Table(data)
new_data, new_header = [100, 99], 'col3'
new_df = df.add(new_data, new_header)
print(new_df.data())
"""
	







