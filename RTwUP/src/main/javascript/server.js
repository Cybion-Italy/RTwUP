/**
 * New node file
 */

require.paths.unshift(__dirname + '/lib');

var fs = require('fs'),
    ws = require('ws'),
    sys = require('sys'),
    url = require('url'),
    http = require('http'),
    path = require('path'),
    mime = require('mime'),
    redis = require('redis');

var db = redis.createClient(6379, 'localhost');

var httpServer = http.createServer( function(request, response) {
    var pathname = url.parse(request.url).pathname;
    if (pathname == "/") pathname = "index.html";
    var filename = path.join(process.cwd(), 'public', pathname);

    path.exists(filename, function(exists) {
        if (!exists) {
            response.writeHead(404, {"Content-Type": "text/plain"});
            response.write("404 Not Found");
            response.end();
            return;
        }

        response.writeHead(200, {'Content-Type': mime.lookup(filename)});
        fs.createReadStream(filename, {
            'flags': 'r',
            'encoding': 'binary',
            'mode': 0666,
            'bufferSize': 4 * 1024
        }).addListener("data", function(chunk) {
            response.write(chunk, 'binary');
        }).addListener("close",function() {
            response.end();
        });
    });
});


var server = ws.createServer({server: httpServer});

db.on("subscribe", function(channel, message){ 
	try{ 
		var ranking = JSON.parse(message);
	}catch (SyntaxError) {return false;}
	server.broadcast(ranking);
});

db.subscribe("RTwUP");

server.listen(8000);
