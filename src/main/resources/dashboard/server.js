var express = require('express');
var bodyParser = require('body-parser');
var path = require('path');
var app = express();
app.use('/', express.static(__dirname + '/public'));
app.set('views', path.join(__dirname, '/public'));
app.use('/bower_components',  express.static(__dirname + '/bower_components'));
app.use('/js',  express.static(__dirname + '/public/js'));
app.engine('html', require('ejs').renderFile);
app.set('view engine', 'ejs');
app.use(bodyParser.json());
var server = require('http').createServer(app);
var io = require('socket.io')(server);

app.get('/2', function (req, res) {
   res.render('optimization.html');
});

//Propagate loctaion information and potential area violation
app.post('/location', function (req, res) {
   console.log(req.body);
   io.sockets.emit('location', req.body);
   res.send('success');
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

// taxi has stopped driving
app.post('/stop', function (req, res) {
    console.log(req.body);
    io.sockets.emit('stop', req.body);
    res.send('success');
});



server.listen(3000, function () {
   console.log('Server started on port 3000')
});
