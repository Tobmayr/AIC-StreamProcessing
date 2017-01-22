
var map;
var markers = [];
var incidents = [];
var violations = [];
var taxiCount="0";
var overallDistance="0";

// handle requests for updating the UI
var socket = io.connect('http://localhost:3000');
setInterval(function(){
    reloadUIElements();
},  1000);

socket.on('add', function (data) {
    createOrMoveMarker(map, data.taxiId, data.latitude, data.longitude);
});

socket.on('stats', function (data) {
    taxiCount=data.taxiCount;
    overallDistance=parseFloat(data.distance).toFixed(2);
});

socket.on('violation', function (data) {
    violations[data.taxiId] = data.distance;
    if (data.distance >= 15.00) {
        removeTaxi(data.taxiId);
    }
});

socket.on('stop', function (data) {
    removeTaxi(data.taxiId);
});

socket.on('incident', function (data) {
    incidents[data.taxiId] = data.speed;
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

    // taxi warning area begin
    var cityCircle = new google.maps.Circle({
        strokeColor: '#FF00C1',
        strokeWeight: 2,
        fillOpacity: 0.05,
        map: map,
        center: forbiddenCity,
        radius: 10000
    });

    // taxi warning area end (prohibited area)
    var cityCircle = new google.maps.Circle({
        strokeColor: '#FF0000',
        strokeWeight: 4,
        fillOpacity: 0.0,
        map: map,
        center: forbiddenCity,
        radius: 15000
    });
};

function createOrMoveMarker(map, taxiId, lat, lng) {
    if (markers[taxiId] != undefined) {
        markers[taxiId].setPosition(new google.maps.LatLng(lat, lng));
        return;
    }
    var icon = {
        url: "img/car.png",
        scaledSize: new google.maps.Size(40, 40), // scaled size
        origin: new google.maps.Point(0, 0),
        anchor: new google.maps.Point(0, 0)
    }
    var marker = new google.maps.Marker({position: new google.maps.LatLng(lat, lng), map: map, icon: icon});
    markers[taxiId] = marker;
};


function reloadUIElements(){
    $("#currentlyDrivingTaxis").text(taxiCount);
    $("#overallDistance").text(overallDistance);

    $("#areaViolations").empty();
    for (var taxiId in violations) {
        var distance = parseFloat(violations[taxiId]).toFixed(2);
        $("#areaViolations").append("<tr><td>" + taxiId + "</td><td>" + distance + "</td></tr>");
    }

    $("#speedingIncidents").empty();
    for (var taxiId in incidents) {
        var speed = parseFloat(incidents[taxiId]).toFixed(2);
        var over = parseFloat(speed - 50.0).toFixed(2);
        $("#speedingIncidents").append("<tr><td>" + taxiId + "</td><td>" + speed + "</td><td class='speeding'>+" + over + "</td></tr>");
    }
}
function removeTaxi(taxiId) {
    if (markers[taxiId] != undefined) {
        markers[taxiId].setMap(null);
    } //Remove all taxi references
    delete markers[taxiId];
    delete violations[taxiId];
    delete incidents[taxiId];


};

