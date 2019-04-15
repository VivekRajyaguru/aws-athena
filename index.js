const express = require('express');
const bodyParser = require('body-parser');
const app = express();
const AWS = require("aws-sdk");

AWS.config.update({region: 'us-east-1'});

const ses = new AWS.SES({ apiVersion: "2010-12-01" });
const athena = new AWS.Athena();
const athenaClient = require("athena-client");

const clientConfig = {
    bucketUri: 's3://perusal-rk/athena-result'
};
const awsConfig = {
    region: 'us-east-1'
};
const client = athenaClient.createClient(clientConfig, awsConfig)

app.use(bodyParser.json())
app.use(bodyParser.urlencoded({ extended: false }))
app.use(bodyParser.text())                                    
app.use(bodyParser.json({ type: 'application/json'}))  


app.post('/query', (req, res) => {
    const params = {
        QueryString: `select * from temp_data limit 10`,
        QueryExecutionContext: {
            Database: 'weather'
        },
        WorkGroup: 'primary',
        ResultConfiguration: {
            OutputLocation: 's3://perusal-rk/athena-result/'
        }
    };
    console.log(params);
    athena.startQueryExecution(params, (err, data) => {
        console.log('err', err);
        console.log('data', data);
        res.send({err, data});
    });
});

app.get('/getExecutionStatus/:id', (req, res) => {
    const params = {
        QueryExecutionId: req.params.id 
      };
      athena.getQueryExecution(params, function(err, data) {
        if (err) console.log(err, err.stack);
        else     console.log(data);          
        res.send({err, data});
      });
});

// Athena Built in Response
/**
 * {
    "err": null,
    "data": {
        "UpdateCount": 0,
        "ResultSet": {
            "Rows": [
                {
                    "Data": [
                        {
                            "constCharValue": "time"
                        },
                        {
                            "constCharValue": "accel_x"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "time (S)"
                        },
                        {
                            "constCharValue": "accel_x (N)"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:26.357Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:27.335Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:28.944Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:29.387Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:30.295Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:31.392Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:32.442Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:33.606Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                },
                {
                    "Data": [
                        {
                            "constCharValue": "2019-01-04T21:02:34.299Z"
                        },
                        {
                            "constCharValue": "0"
                        }
                    ]
                }
            ],
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {
                        "CatalogName": "hive",
                        "SchemaName": "",
                        "TableName": "",
                        "Name": "time",
                        "Label": "time",
                        "Type": "constchar",
                        "Precision": 2147483647,
                        "Scale": 0,
                        "Nullable": "UNKNOWN",
                        "CaseSensitive": true
                    },
                    {
                        "CatalogName": "hive",
                        "SchemaName": "",
                        "TableName": "",
                        "Name": "accel_x",
                        "Label": "accel_x",
                        "Type": "constchar",
                        "Precision": 2147483647,
                        "Scale": 0,
                        "Nullable": "UNKNOWN",
                        "CaseSensitive": true
                    }
                ]
            }
        }
    }
}
 */
app.get('/getQueryExecutionResult/:id', (req, res) => {
    const params = {
        QueryExecutionId: req.params.id,
        // MaxResults: 'NUMBER_VALUE',
        // NextToken: 'STRING_VALUE'
    };
    athena.getQueryResults(params, function(err, data) {
        if (err) console.log(err, err.stack);
        else     console.log(data);
        res.send({err, data});
    });
});

// From Reference 
// https://www.npmjs.com/package/athena-express
app.get('/executeQuery', (req, res) => {
    client.execute('select * from weather.temp_data limit 100', (err, data) => {
        console.log('err', err);
        console.log('data', data);
        res.send({err, data});
    });
});

const generateColumnString = (columnData) => {
    let columnString = '';
    if (columnData) {
        columnData.map( (element) => {
            if (columnString === '') {
                columnString += `'${element.name}' ${element.type}`
            } else {
                columnString += `,'${element.name}' ${element.type}`
            }
        });
    }
    return columnString;
};

/**
 * location = s3://temp-csv-data/csv
 * tableName = 'new_data_table'
 *  columnData = [{
 * 'name': 'time',
 * 'type': 'string'
 * },
 * {
 * 'name': 'accel_x',
 * 'type': 'string'
 * }]
 * 
 * */ 
app.post('/createTable', (req, res) => {
    const {body: {tableName, columnData, location}} = req;
    const columnString = generateColumnString(columnData);
    const queryString = `
    CREATE EXTERNAL TABLE ${tableName}( ${columnString} )
      ROW FORMAT DELIMITED 
        FIELDS TERMINATED BY ',' 
      STORED AS INPUTFORMAT 
        'org.apache.hadoop.mapred.TextInputFormat' 
      OUTPUTFORMAT 
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      LOCATION
        '${location}'
    `;
    res.send(queryString);
});

const server = app.listen(8080, () => {});