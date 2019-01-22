'use strict;'

var dataForge = require('data-forge');
require('data-forge-fs');

var df = dataForge.readFileSync("points.csv").
    parseCSV().
    parseFloats(["ZLATITUDE", "ZLONGITUDE", "ZACCURACY"]).
    dropSeries(["Z_PK", "Z_ENT","Z_OPT", "ZIDENTIFIER", "ZMETHOD", "ZPACKET_DROP"])

let transformed = df.select(row => {
    const clone = Object.assign({}, row);
    clone["date"] = new Date(clone["ZDATETIME"] * 1000);
    return clone;
}).
orderBy(row => row.date).
dropSeries("ZDATETIME").
where(row => row["date"] > new Date("2018-06-01T06:00:00.000Z")).
where(row => row["date"] < new Date("2018-06-16T06:00:00.000Z"))

var latest = {}
transformed.
forEach(row => {
	var day = row.date.getUTCDate()
	if(!latest[day]){
		latest[day] = row.date
	} else {
		if(latest[day] < row.date){
			latest[day] = row.date
		}
	}
})
var evenings = Object.values(latest).map(x => x.toISOString())

let transformed2 = transformed.select(row => {
    const clone = Object.assign({}, row);
    clone["value"] = [clone["ZLONGITUDE"], clone["ZLATITUDE"], clone["ZACCURACY"]]
    clone["evening"] = evenings.indexOf(row.date.toISOString()) > 0
    return clone;
}).
dropSeries(["ZLATITUDE", "ZLONGITUDE","ZACCURACY"])



transformed2
.where(row => row["ZDEVICEIDENTIFIER"] === "18b79efff0085f85")
.dropSeries("ZDEVICEIDENTIFIER")
.asJSON()                            // Write out data file in CSV (or other) format.
.writeFileSync("output.json");