var express = require('express');
var bodyParser = require('body-parser');
var app = express();
app.use(express.static(__dirname + '/public'));
app.use('/bower_components',  express.static(__dirname + '/bower_components'));
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.use(bodyParser.json());
var server = require('http').createServer(app);
var io = require('socket.io')(server);


app.get('/', function (req, res) {
   res.render('index.html');
});

// add new taxis or update an existing ones
app.post('/add', function (req, res) {
   console.log(req.body);
   io.sockets.emit('add', req.body);
   res.send('success');
});

// update number of currently driving taxis and overall distance
app.post('/stats', function (req, res) {
   console.log(req.body);
   io.sockets.emit('stats', req.body);
   res.send('success');
});

// area violations
app.post('/violation', function (req, res) {
   console.log(req.body);
   io.sockets.emit('violation', req.body);
   res.send('success');
});

// speeding incidents
app.post('/incident', function (req, res) {
   console.log(req.body);
   io.sockets.emit('incident', req.body);
   res.send('success');
});

server.listen(3000);