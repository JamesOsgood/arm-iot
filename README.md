# arm-iot

## Setup

Create a free M0 [MongoDB Atlas cluster](https://www.mongodb.com/cloud/atlas) in the cloud provider of your choice.

Install Python 3 and the following packages pymongo, dnspython.

Copy `connection_string.txt.template` to `connection_string.txt` and paste in the Atlas connection string as documented [here](https://docs.atlas.mongodb.com/getting-started/?_ga=2.233944087.82986543.1561906829-943158870.1536563311)

You can then run the queries below and view the results using the community edition of [MongoDB Compass](https://www.mongodb.com/products/compass)

## Queries

Find any bucket document with a measurement where pressure >= 1040

```javascript
db.schema_demo5.find({'measurements.pressure' : { '$gte' : 1040 }})
```

Find any bucket document with a measurement where pressure >= 1040 and filter the sub measurements to only include those readings

```javascript
db.schema_demo5.aggregate(
    [
      {
        $match: { "measurements.pressure" : { $gte : 1040 } }
      }, 
      {
        $project: 
          {
            measurement : 
              {
                  $filter : 
                  {
                    input : "$measurements",
                    as : 'item',
                    cond : { $gte : ['$$item.pressure', 1040] }
                  }
              },
            sensor_id : 1,
            count : 1
          }
        }
      ]
)
```

## Reactive code

Use an Atlas Trigger to respond to a particular document update

![Trigger](img/trigger.png)

Use a match expression like

```javascript
{ "fullDocument.measurements.pressure" : { "$gte" : 1050 } }
```

And a function like

```javascript
exports = function(changeEvent) {

  console.log(changeEvent);
  
  var collection = context.services.get("iot-demo").db("iot").collection("alerts");
  var alert_message = { 'message' : 'Pressure has gone above value', 'm' : changeEvent.fullDocument };
  collection.insertOne(alert_message);
  
};
```
