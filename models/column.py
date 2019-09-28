import csv, re, pymysql, psycopg2, psycopg2.extras, copy, json
from datetime import datetime, date

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
