var map;
var markers = [];
var incidents = [];

// handle requests for updating the UI
var socket = io.connect('http://localhost:3000');

socket.on('add', function (data) {
    for (var i = 0; i < data.length; i++) {
        createOrMoveMarker(map, data[i].taxiId, data[i].latitude, data[i].longitude);
    }
    ;
});

socket.on('driving', function (data) {
    $("#currentlyDrivingTaxis").text(data.nrOfTaxis);
});

socket.on('distance', function (data) {
    $("#overallDistance").text(data.distance + ' km');
});

socket.on('violation', function (data) {
    for (var i = 0; i < data.length; i++) {
        $("#areaViolations").append('<li>Taxi ' + data[i].taxiId + '</li>');
    }
    ;
});

socket.on('incident', function (data) {
    for (var i = 0; i < data.length; i++) {
        incidents[data[i].taxiId] = data[i].speed;
    }
    $("#speedingIncidents").empty();
    for (var taxiId in incidents) {
        $("#speedingIncidents").append('<li>Taxi ' + taxiId + ' (' + incidents[taxiId] + ' km/h)' + '</li>');
    }


});


// google maps
function initMap() {
    var forbiddenCity = new google.maps.LatLng(39.916320, 116.397155);
    var mapCanvas = document.getElementById("map");
    var mapOptions = {
        center: forbiddenCity,
        zoom: 11
    };
    map = new google.maps.Map(mapCanvas, mapOptions);

    // taxi area
    var cityCircle = new google.maps.Circle({
        strokeColor: '#FF0000',
        map: map,
        center: forbiddenCity,
        radius: 10000
    });

};

function createOrMoveMarker(map, taxiId, lat, lng) {
    if (markers[taxiId]!=undefined){
        markers[taxiId].setPosition(new google.maps.LatLng(lat, lng));
        return;
    }
    var icon = {
        url: "img/car.png",
        scaledSize: new google.maps.Size(20, 20), // scaled size
        origin: new google.maps.Point(0, 0),
        anchor: new google.maps.Point(0, 0)
    }
    var marker = new google.maps.Marker({position: new google.maps.LatLng(lat, lng), map: map, icon: icon});
    markers[taxiId]=marker;
};

function removeMarker(map, taxiId) {
    //TODO
};

