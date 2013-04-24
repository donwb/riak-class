var fs = require('fs');
var readline = require('readline');
var db = require('riak-js').getClient()
var db = require('riak-js').getClient({host: "127.0.0.1", port: "10018"});

var count = 0;

var load = function test(bucketName) {
	var start = new Date();

	fs.readFile('subset.csv', 'utf-8', function(err, data) {
		var d = data.split('\n');
		
		console.log('starting....');
		count = 0;

		for(var i=0;i<d.length;i++) {
			var record = d[i].split(',');
			var obj = {
				"Dna":record[0], 
				"Gender":record[1],
				"GivenName":record[2],
				"MiddleInitial": record[3],
				"Surname":record[4],
				"StreetAddress":record[5],
				"City":record[6],
				"State":record[7],
				"ZipCode":record[8],
				"TelephoneNumber":record[9],
				"Birthday":record[10],
				"NationalID":record[11],
				"Occupation":record[12],
				"BloodType":record[13],
				"Pounds":record[14],
				"FeetInches":record[15],
				"Latitude":record[16],
				"Longitude":record[17]
			};
				

			db.save(bucketName, i, obj);
			count += 1;
		}
		console.log('count: ' + count);
		console.log('start: ' + start);
	});

}

var load2 = function load2(bucketName) {
	var start = new Date();
	count = 0;

	var rd = readline.createInterface({
		input: fs.createReadStream('subset.csv'),
		output: process.stdout,
		terminal: false
	});
	console.log('Started on: ' + start);

	rd.on('line', function(line){
		var record = line.split(',');
		var obj = {
			"Dna":record[0], 
			"Gender":record[1],
			"GivenName":record[2],
			"MiddleInitial": record[3],
			"Surname":record[4],
			"StreetAddress":record[5],
			"City":record[6],
			"State":record[7],
			"ZipCode":record[8],
			"TelephoneNumber":record[9],
			"Birthday":record[10],
			"NationalID":record[11],
			"Occupation":record[12],
			"BloodType":record[13],
			"Pounds":record[14],
			"FeetInches":record[15],
			"Latitude":record[16],
			"Longitude":record[17]
		};
		//console.log(JSON.stringify(obj));
		db.save(bucketName, count, obj);
		count += 1;
	});

}

var runQuery = function runQuery(bucketName) {
	db.get(bucketName, '1', function(err, zombies, meta) {
		console.log('result: ' + JSON.stringify(zombies));
	});
}

var search = function search(bucketName) {

	// this shit broke
	console.log('searching: ' + bucketName);
	db.mapreduce.add(bucketName)
		.map('Riak.mapByFields', {"Gender":"male"})
		.reduce(['Riak.reduceSum', function(value, count) {
			return count += 1;
		}])
		.run(function(err, res) {
			console.log(res);
		}
	)
}


var args = process.argv;

if(args[2] === 'query') {
	runQuery(args[3]);
} else if(args[2] === 'search') {
	search(args[3]);
} else {
	load2(args[2]);
}

process.on('exit', function() {
	console.log('count: ' + count);
	var d = new Date();
	console.log('end: ' + d);
})





