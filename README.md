# arm-iot

## Queries

Find any bucket document with a measurement where pressure >= 1040

```
db.schema_demo5.find({'measurements.pressure' : { '$gte' : 1040 }})
```

Find any bucket document with a measurement where pressure >= 1040 and filter the sub measurements to only include those readings

```
db.schema_demo5.aggregate(
    [{$match: {
    "measurements.pressure" :
     { $gte : 1040 }
    }}, {$project: {
    elems : {
        $filter : 
        {
        input : "$measurements",
        as : 'item',
        cond : { $gte : ['$$item.pressure', 1040] }
        }
    },
    sensor_id : 1,
    count : 1
    }}]
)
```