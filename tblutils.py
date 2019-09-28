import csv, re, pymysql, psycopg2, psycopg2.extras, copy, json
from datetime import datetime, date

class Table():
	"""
	A helper class for working with table data.

	I/O
		- read_csv
		- load_lists (list of lists)
		- load_dicts (lsit of dicts)
		- save
	Mutate
		- select
		- desc
		- filter
		- sort
		- add
		- append

	Database
		- connect
		- create table
		- write to table
		
	"""

	def __init__(self, data=None, data_type=None, headers=True):
		if data is not None:
			self.load(data, data_type, headers)
		else:
			self._data = []

	def __repr__(self):
		return'{} rows <> {}'.format(len(self), self.headers())

	def __getitem__(self,col):
		return self._data[col]

	def __setitem__(self, idx, col):
		self._data[idx] = col

	def __len__(self):
		return 0 if len(self._data) == 0 else len(self._data[0])

	def headers(self):
		return [col.label for col in self._data]

	def data(self, _format='dict'):
		if len(self)==0:
			return []
		else:
			if _format == 'list':
				# [[], [], []]
				tmp = [[col.label for col in self._data]]
				i = 0
				while i < len(self):
					row  = [col[i] for col in self._data]
					tmp.append(row)
					i += 1
			elif _format == 'dict':
				# [{}, {}, {}]
				tmp = []
				for i, col in enumerate(self._data):
					for idx, val in enumerate(col):
						if i == 0:
							tmp.append({col.label:val})
						else:
							tmp[idx][col.label] = val
			return tmp

	def copy(self):
		return copy.deepcopy(self)

	## LOADER
	def load(self, data, data_type=None, headers=True):
		if data_type == None:
			data_type = self._get_data_type(data)
		# router to use appropriate loader function
		if data_type == 'csv':
			self._data = self._read_csv(data, headers)
		elif data_type == 'list':
			self._data = self._load_lists(data, headers)
		elif data_type == 'dict':
			self._data = self._load_dicts(data)
		elif data_type == 'json':
			self._data = self._read_json(data)
		elif data_type == 'column':
			self._data = self._load_columns(data)
		else:
			raise ValueError("'{}' is not a supported data type".format(data_type))

	def _get_data_type(self, data):
		if isinstance(data, list):
			if isinstance(data[0], list):
				return 'list'
			if isinstance(data[0], Column):
				return 'column'
			elif isinstance(data[0], dict):
				return 'dict'
		elif isinstance(data, str): # for parsing file names
			search = re.search('.+\.(\w*)', data)
			return search.group(1)
		else:
			return False

	def _read_csv(self, filename, headers=True):
		with open(filename) as f:
			reader = csv.reader(f)
			tmp_data = []
			for i, row in enumerate(reader):
				if i == 0:
					if headers==True:
						for item, col in enumerate(row):
							tmp_data.append(Column([],label=col))
					else:
						for item, col in enumerate(row):
							tmp_data.append(Column([], label=f'Column{item}'))
				else:
					for item, col in enumerate(row):
						tmp_data[item].append(row[item])
			return tmp_data

	def _read_json(self, filename, headers=True):
		with open(filename) as f:
			x = json.loads(f.read())
			if isinstance(x, list):
				if isinstance(x[0], dict):
					return self._load_dicts(x)
				elif isistance(x[0], list):
					return self._load_lists(x, headers)
			raise ValueError('Data should be list of lists/dicts')

	def _load_columns(self, data):
		tmp_data = []
		headers = []
		length = len(data[0])
		for col in data:
			if len(col) != length:
				raise ValueError('Columns are unequal lengths')

			if col.label == '':
				flag = False
				i = 1
				while flag != True:
					if f'Column{i}' not in headers:
						flag = True
						col.label = f'Column{i}'
					i += 1
				tmp_data.append(col)
				headers.append(col.label)
		return data

	def _load_lists(self, data, headers=True):
		tmp_data = []
		for i, row in enumerate(data):
			if headers == True and i == 0:
				for item, col in enumerate(row):
					tmp_data.append(Column([],label=col))
			elif i ==0:
				for item, col in enumerate(row):
					tmp_data.append(Column([],label=f'Column{item}'))
			else:
				for item, col in enumerate(row):
					tmp_data[item].append(row[item])
		return tmp_data


	def _load_dicts(self, data):
		tmp_data = [Column([], label=label) for label in data[0].keys()]
		for row in data:
			for i, label in enumerate(data[0].keys()):
				tmp_data[i].append(row[label])
		return tmp_data


	## MUTATORS
	def _get_col_index(self, col):
		## get list of indices for a column
		if not isinstance(col, str):
			raise ValueError("Column names must be strings")
		i = 0
		match_flag = False
		while i < len(self._data):
			if col == self._data[i].label:
				return i
			i += 1
		raise ValueError("'{}' is not a valid column name".format(col))

	def select(self, *cols):
		## produce a subset based on a list of column names
		indices = [self._get_col_index(col) for col in cols]
		if len(cols) == 1:
			return self._data[indices[0]]
		new = Table()
		new._data = [self._data[i] for i in indices]
		return new

	def filter(self, *bool_lists):
		"""
		usage: 
		d1 = data.filter(data.select('First Name') == 'Grayden')
		d1 = data.filter([name in ('Grayden', 'Tom') for name in df.select('First Name')], [data.select('Cohort') in ('ogre', 'opal')])
		"""
		indices = []
		tmp = self.copy()
		tmp._data = []
		i = 0
		while i < len(self):
			row = []
			for _list in bool_lists:
				row.append(_list[i])
			if all(row):
				indices.append(i)
			i += 1
		for col in self._data:
			data = [col[idx] for idx in indices]
			tmp._data.append(Column(data, col.label))
		return tmp

	def sort(self, *cols):
		tmp = self.copy()
		for i, col in enumerate(tmp):
			tmp[i] = Column([], col.label)
		first_col = tmp.select(cols[0]) # get the first column to sort by
		i = 0
		indices = []
		while i < len(self):
			row = {col.label:col[i] for col in self} # row to insert
			val = row[first_col.label] # first val
			idx = first_col._binary_search(val) # binary search to place the new value
			if len(first_col) == i or val == first_col[i]: # new value is equal to the value currently at that index
				tmp.insert(self._place_row(row, tmp, idx, 0, cols), row)
			else: # new value is not different from adjacent value
				tmp.insert(idx, row)
			first_col = tmp.select(cols[0])
			i += 1
		return tmp


	def _place_row(self, row, data, row_idx, col_idx, cols):
		col = data.select(cols[col_idx])
		new_val = row[col.label]
		#print(len(col), row_idx)
		if len(col) == row_idx or new_val < col[row_idx]:
			return row_idx
		else:
			if col_idx == len(cols)-1: # full row match
				return self._place_row(row, data, row_idx+1, 0, cols) # advance to next row
			else:
				return self._place_row(row, data, row_idx, col_idx+1, cols) # check next column


	def desc(self):
		if len(self) > 0:
			i = 0
			tmp = self.copy()
			data = self.data()
			buf = []
			for n in range(len(self)-1, -1, -1):
				buf.append(data[n])
			tmp.load(buf)
			return tmp
		else:
			return 


	def add(self, col):
		if len(col) != len(self):
			raise ValueError("Input size doesn't match existing data.")
		else:
			if col.label == '':
				flag = False
				i = 1
				while flag != True:
					if f'Column{i}' not in self.headers():
						flag = True
						col.label = f'Column{i}'
					i += 1
				self._data.append(col)
		return self

	def append(self, row):
		if isinstance(row, dict):
			for col in self:
				if col.label in row.keys():
					col.append(row[col.label])
				else:
					col.append(None)
		elif isinstance(row, (list, tuple)):
			if len(row) != len(row):
				raise ValueError('Different lengths. New row should specify value for each column')
			for i, col in enumerate(self._data):
				col.append(row[i])
		else:	
			raise TypeError('Data type not supported -- format as list or dictionary')


	def insert(self, idx, row):
		if isinstance(row, dict):
			for col in self:
				if col.label in row.keys():
					col.insert(idx, row[col.label])
				else:
					col.insert(idx, None)
		elif isinstance(row, (list, tuple)):
			if len(row) != len(row):
				raise ValueError('Different lengths. New row should specify value for each column')
			for i, col in enumerate(self):
				col.insert(idx, row[i])
		else:	
			raise TypeError('Data type not supported -- format as list or dictionary')
				

	## WRITER
	def save(self, fn):
		data_type = self._get_data_type(fn)
		if data_type == 'csv':
			self._write_csv(fn)
		elif data_type == 'json':
			self._write_json(fn)
		else:
			raise ValueError('Unable to save in that format, .csv and .json supported')
		return True

	def _write_csv(self, fn):
		with open(fn, 'w') as f:
			writer = csv.writer(f)
			writer.writerow(self.headers())
			i = 0
			while i < len(self):
				writer.writerow([col[i] for col in self._data])
				i += 1
		return True

	def _write_json(self, fn):
		with open(fn, 'w') as f:
			f.write(json.dumps(self.data()))
		return True


	## DATABASE
	def open_connection(self, rdbms='postgres', host=None, port=None, user=None, passwd=None, db=None):
		if rdbms == 'postgres':
			self.db = psycopg2.connect(host=host,port=port, user=user,password=passwd,dbname=db)
		elif rdbms == 'mysql':
			self.db = pymysql.connect(host=host,port=port, user=user,passwd=passwd,db=db)
		else:
			raise ValueError('{} is not a supported database.'.format(rdbms))
		self.cur = self.db.cursor()
		return True

	def close_connection(self):
		self.db.close()

	def write_to_db(self, table_name):
		col_str = ', '.join([col.label.lower().replace(' ','_') for col in self._data])
		token_str = ', '.join(['%s']*len([col.label for col in self._data]))
		sql = '''INSERT INTO {} ({}) VALUES ({});'''.format(table_name, col_str, token_str)
		for row in self.data('list')[1:]:
			data = row
			self.cur.execute(sql, data)
		self.db.commit()


	def _write_create_table(self, table_name, primary_key=None):
		type_list = [col._get_data_type() for col in self._data]
		sql = 'create table {} (\n'.format(table_name)
		col_string = ''
		if primary_key is None:
			sql += '{}_id serial primary key,\n'.format(table_name)
		for i, col in enumerate(self._data):
			col_string += '{} {}'.format(col.label.lower().replace(' ','_'), col._type)
			if col.label == primary_key:
				col_string += ' primary key,\n'
			elif i < len(self._data) - 1:
				col_string += ',\n'
		sql += col_string
		sql += ');'	
		return sql

	def make_db_table(self, table_name, primary_key=None):
		sql = self._write_create_table(table_name, primary_key)
		self.cur.execute(sql)
		self.db.commit()
		self.write_to_db(table_name, [col.label for col in self._data])
		return true



	## GOOGLE SHEETS

	## STATS

class Column():

	def __init__(self, data=None, label='', _type=None):
		self.label =  str(label)
		if data==None:
			self._data = []
		else:
			self._data = list(data)
		if self._data is not None and _type is None and len(self._data) > 0:
			self._type = self._get_data_type()
		else:
			self._type = _type

	def __repr__(self):
		if self.label != '':
			return "{}: {}".format(self.label, self._data)
		else:
			return f"{self._data}"

	def __getitem__(self, idx):
		return self._data[idx]

	def __setitem__(self, idx, x):
		if idx < len(self):
			self._data[idx] = x
		else:
			self._data.insert(idx, x)

	def __len__(self):
		return len(self._data)

	def __contains__(self, val):
		return True if val in self._data else False
		
	def __eq__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):

				if isinstance(self[i], (str,)):
					if self[i].lower() != other[i].lower():
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i] != other[i]:
						tmp.append(False)
					else:
						tmp.append(True)
					i += 1
		else:
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] != other:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() != other.lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		return tmp

	def __ne__(self,other):
		return [not item for item in self==other]

	def __lt__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] >= other[i]:
						tmp.append(False)
					else:
						tmp.append(True)
					i += 1
				else:
					if self[i].lower() >= other[i].lower():
						tmp.append(False)
					else:
						tmp.append(True)
		else:
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] >= other:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() >= other.lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		return tmp


	def __le__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] > other[i]:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() > other[i].lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		else:
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] > other:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() > other.lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		return tmp


	def __ge__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] < other[i]:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() < other[i].lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		else:
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] < other:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() < other.lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		return tmp

	def __gt__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] <= other[i]:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() <= other[i].lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		else:
			while i < len(self):
				if isinstance(self[i], (str,)):
					if self[i] <= other:
						tmp.append(False)
					else:
						tmp.append(True)
				else:
					if self[i].lower() <= other.lower():
						tmp.append(False)
					else:
						tmp.append(True)
				i += 1
		return tmp

	def __add__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				tmp.append(self[i] + other[i])
				i += 1
		else:
			while i < len(self):
				tmp.append(self[i] + other)
				i += 1
		return tmp

	def __sub__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				tmp.append(self[i] - other[i])
				i += 1
		else:
			while i < len(self):
				tmp.append(self[i] - other)
				i += 1
		return tmp

	def __mul__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				tmp.append(self[i] * other[i])
				i += 1
		else:
			while i < len(self):
				tmp.append(self[i] * other)
				i += 1
		return tmp

	def __div__(self, other):
		i = 0
		tmp = []
		if isinstance(self, (type(other), list, tuple)):
			if len(self) != len(other):
				raise ValueError('Columns of different lengths')
			while i < len(self):
				tmp.append(self[i] / other[i])
				i += 1
		else:
			while i < len(self):
				tmp.append(self[i] / other)
				i += 1
		return tmp

	def filter(self,expr):
		i = 0
		tmp = Column()
		while i < len(self._data):
			if expr[i]:
				tmp.append(self._data[i])
			i += 1
		return tmp

	def append(self, x):
		self._data.append(x)

	def insert(self, idx, val):
		self._data.insert(idx, val)

	def _binary_search(self, x, start=0, end=None):
		# x = value
		# start = lower search bound
		# end = upper search bound

		# set default for convenience
		if end == None:
			end = len(self)

		# to handle values that aren't in the Column
		if start == end:
			return start

		# recursive search
		mid = (end + start)//2
		if isinstance(x, (str,)):
			if x.lower() < self[mid].lower():
				return self._binary_search(x, start, mid)
			elif x.lower() > self[mid].lower():
				return self._binary_search(x, mid+1, end)
			else:
				return self._data.index(self[mid]) # return lowest index with this value
		else:
			if x < self[mid]:
				return self._binary_search(x, start, mid)
			elif x > self[mid]:
				return self._binary_search(x, mid+1, end)
			else:
				return self._data.index(self[mid]) # return lowest index with this value

	def sort(self):
		# insertion sort using binary search
		tmp = Column()
		for i, val in enumerate(self._data):
			idx = tmp._binary_search(val)
			tmp.insert(idx, val)

		return tmp

	def copy(self):
		return copy.deepcopy(self)


	def _get_data_type(self):
		# assumes data consistency, first non-null value is all that is needed
		# Integer, Float/Decimal, Date, Datetime, Character
		if len(self) == 0:
			raise Exception('No values to determine type')
		i = 0
		test_val = None
		while i < len(self):
			if self[i] is not None:
				test_val = self[i]
				i = len(self)
			i +=1			
		if test_val == None:
			raise Exception('No values to determine type')
		try:
			int(test_val)
			if int(test_val) == float(test_val):
				return 'integer'
			else:
				digits = len(str(test_val))
				decimals = len(str(test_val).split('.')[1])
				return 'numeric ({}, {})'.format(digits)
		except:
			try:
				float(test_val)
				return 'numeric'
			except:
				# non-numerical
				date = self._parse_date(test_val)
				if date != False:
					return date[0]
				else:
					return 'text'

	@staticmethod
	def _parse_date(val):
		try:
			x = datetime.strptime(val, '%Y-%m-%d')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%d-%m-%Y')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%m-%d-%Y')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%Y/%m/%d')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%d/%m/%Y')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%m/%d/%Y')
			return ('date', x)
		except:
			pass
		try:
			x = datetime.strptime(val, '%Y-%m-%dT%H:%M:%S')
			return ('datetime', x)
		except Exception as e:
			pass
		return False


if __name__ == "__main__":
	#df = Table('student_data.csv')
	#df1 = df.filter(df.select('First Name') != 'Grayden', df.select('Cohort') == 'ibex').sort('Cohort', 'First Name').select('Cohort', 'First Name').desc()

	#df1.append({'First Name': 'Grayden', 'Last Name': 'Shanaaaad'})
	#x = df1.select('Cohort') == 'ibex'
	#print(df1.data())

	#print(Table())

	x = Column([1,5,9,15])
	y = Column([1,2,3,4])
	print(x)
	print(y)
	df = Table([x,y])
	print(df.select("Column1"))
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
	







