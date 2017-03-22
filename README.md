# filedump
minimalistic way of storing a big amount of data to the disk

## Usage
```python

# path where the stuff should be stored
path = "path/to/storage"

# maximum number of elements to be stored in a file
n_elems_in_file = 1000
  
# creating a new dump
with FileDump(store_path, n_elems_in_file) as f:
    
    # store stuff
    for i in range(10000):
        f.dump({"foo": "bar {0}".format(i)}

# reusing an old dump
with FileDump(store_path) as f:

    # how much stuff in here?
    print(len(f))
    
    # store other stuff
    for i in range(20000, 30000):
        f.dump({"foo": "bar {0}".format(i)}
    
    # load a particular range
    for elem in f.read(500, 1367):
        print(elem)
    
    # load the all stored information
    for elem in f:
        print(elem)

# classic usage needs closing the dump after usage
f = FileDump(store_path)
f.dump({"hello": "world"})
f.close()

```
