var request = require('request'),
    fs = require('fs'),
    csv = require('csvtojson'),
    elasticsearch = require('elasticsearch');

var config = require('./env.js')

var client = new elasticsearch.Client({
    host: config.elasticHost,
    log: 'trace'
});

var csvFile = config.filename


function readCsvtoBulk(csvFile, callback) {
    var bulkPostbody = []
    csv().fromFile(csvFile)
        .then((jsonObj) => {
            console.log(jsonObj);
            jsonObj.forEach(e => {
                bulkPostbody.push({
                    index: {
                        _index: config.index,
                        _type: config.type,
                        _id: e._id
                    }
                })
                delete e._id
                bulkPostbody.push(e)
            })
            callback(bulkPostbody)
        })
}


readCsvtoBulk(config.filename, function(bulkData) {
    console.log(bulkData)
    client.bulk({
        body: bulkData
    }, function(err, resp) {
        if (err) console.log(err)
    });

})
