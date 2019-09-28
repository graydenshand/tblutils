# Table Utilities ~ *tblutils*

tblutils is a package for working with tables in Python. It provides a variety of utilities to make working with tables easier.

Table data is flat, with columns and rows, like in a relational database.

## Getting Started

tblutils consists of two classes:
* `Column` - The basic data type for series (e.g. `Age: [45, 22, 30]`, `Favorite Color: ['red','green','blue']`)
* `Table` - A data type containing one or more columns (e.g. `3 rows <> [Age, Favorite Color`])


### Loading Data
Tables load data through the `load()` method, which is invoked upon instantiating the class.
For example, if data is in a csv file named "data.csv", it can be loaded during instantiation like:
```
tbl = Table("data.csv")
```
or afterwards like:
```
tbl = Table()
tbl.load("data.csv")
```

In addition to the `data` parameter, `__init__()` and `load()` take two other parameters:
```
load(self, data, data_type=None, headers=True)
__init__(self, data=None, data_type=None, headers=True)
```
* `data_type` *Str::None* - currently supported data types are: `'csv'` and `'json'` for .csv & .json files, and `'list'`,`'dict'`, `'column'` for lists of: lists, dictionaries, columns. If `data_type` is `None`, `load()` invokes the `_get_data_type()` method to guess the data type.
* `headers` *Bool::True* - flag to indicate if headers are included in the dataset (only applicable for .csv and list-of-list data types)



### Saving Data
Currently, the `Table` class supports writing data in .csv and .json formats. The correct format will be detected by the file name you provide via the `_get_data_type()` method.
```
tbl = Table("data.csv")

# to save to a .csv file named 'output.csv'
tbl.save("output.csv")

# to save to a .json file named 'output.json'
tbl.save("output.json")
```
One can immediately see the ease of converting file types.

### Accessing and Displaying Data
The `data()` method will return the serialized contents of the table. It takes one parameter, `_format='dict'`, to indicate whether to return the data as a list of list or a list of dictionaries.

```
x = Column([1,5,9,15])
y = Column([1,2,3,4])
tbl = Table([x,y])
print(table.data()) # defaults to list of dictionaries

###
# [{'Column1': 1, 'Column2': 1}, {'Column1': 5, 'Column2': 2}, {'Column1': 9, 'Column2': 3}, {'Column1': 15, 'Column2': 4}]
###

print(tbl.data('list')) # list of lists

###
# [['Column1', 'Column2'], [1, 1], [5, 2], [9, 3], [15, 4]]
###
```

Columns within a table can be accessed in two ways: 
1) The first is to use their integer index and standard slice notation (e.g. `tbl[0] # returns first column`). Note, this means that `enumerate(tbl)` is a column-wise generator, row-wise enumeration can be achieved via `enumerate(tbl.data())`.
2) Secondly, they can be accessed by their `.label` (column name) through the table's `select()` method (e.g. `tbl.select('Column1')`). Multiple labels can be passed to `select()` and a new table is returned containing only the specified columns.
