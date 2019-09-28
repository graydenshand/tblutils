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
* `data_type` *Str::None* - currently supported data types are .csv & .json files, and lists of: lists, dictionaries, columns
* `headers` *Bool::True* - flag to indicate if headers are included in the dataset (only applicable for .csv and list-of-list data types)
if `data_type` is `None`, `load()` invokes the `_get_data_type()` method to guess the data type.


### Saving Data
Currently, the `Table` class supports writing data in .csv and .json formats. The correct format will be detected by the file name you provide via the `_get_data_type()` method.
```
tbl = Table("data.csv")

# to save to a .csv file named 'output.csv'
tbl.save("output.csv")

# to save to a .json file named 'output.json'
tbl.save("output.json")
```
