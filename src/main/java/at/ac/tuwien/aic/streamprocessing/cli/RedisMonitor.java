package at.ac.tuwien.aic.streamprocessing.cli;

import at.ac.tuwien.aic.streamprocessing.storm.trident.persist.InfoType;
import javafx.application.Application;
import javafx.application.Platform;
import javafx.beans.property.SimpleStringProperty;
import javafx.beans.property.StringProperty;
import javafx.collections.FXCollections;
import javafx.geometry.Insets;
import javafx.geometry.Pos;
import javafx.scene.Scene;
import javafx.scene.control.Label;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.layout.HBox;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.scene.text.Font;
import javafx.scene.text.FontWeight;
import javafx.stage.Stage;
import redis.clients.jedis.Jedis;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.*;
import java.util.stream.Collectors;

public class RedisMonitor extends Application {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");

    private static final String REDIS_HOST = "localhost";
    private static final int REDIS_PORT = 6379;
    private static final int INTERVAL = 2000;

    private final Jedis jedis;

    static {
        DECIMAL_FORMAT.setRoundingMode(RoundingMode.CEILING);
    }

    public RedisMonitor() {
        jedis = new Jedis(REDIS_HOST, REDIS_PORT);
    }

    void run(TableView tableView) {
        while (true) {
            List<TaxiData> taxis = getTaxiIds().stream()
                    .map(this::queryTaxi)
                    .collect(Collectors.toList());
            if(!taxis.isEmpty()) {
                Platform.runLater(new Runnable() {
                    @Override
                    public void run() {
                        tableView.setItems(FXCollections.observableArrayList(taxis));
                        // printData(taxis);
                    }
                });

            }

            try {
                Thread.sleep(INTERVAL);
            } catch (InterruptedException e) {

            }
        }
    }

    private List<Integer> getTaxiIds() {
        Set<Integer> ids = new HashSet<>();

        for (String key : jedis.keys("*")) {
            if (!key.endsWith(InfoType.DISTANCE.getKeyPrefix()) && !key.endsWith(InfoType.AVERAGE_SPEED.getKeyPrefix())) {
                continue;
            }

            Integer id = Integer.parseInt(key.split("_")[0]);
            ids.add(id);
        }

        List<Integer> sortedIds = new ArrayList<>(ids);
        Collections.sort(sortedIds);

        return sortedIds;
    }

    private TaxiData queryTaxi(Integer id) {
        String distance = jedis.get(id.toString() + InfoType.DISTANCE.getKeyPrefix());
        String averageSpeed = jedis.get(id.toString() + InfoType.AVERAGE_SPEED.getKeyPrefix());

        return new TaxiData(
                id.toString(),
                round(distance),
                round(averageSpeed)
        );
    }

    private void printData(List<TaxiData> taxis) {
        System.out.println("\033[H\033[2J");
        System.out.flush();
        System.out.println("Taxi ID\t\tDistance (km)\tAverage Speed (km/h)");
        System.out.println("----------------------------------------------------");
        for (TaxiData taxi : taxis) {
            System.out.println(taxi.id.getValue() + "\t\t" + taxi.distance.getValue() + "\t\t" + taxi.averageSpeed.getValue());
        }
    }

    private String round(String value) {
        if (value == null) {
            return "N/A";
        }

        Double val = Double.parseDouble(value);
        return DECIMAL_FORMAT.format(val);
    }

    public static void main(String[] args) {
        launch(args);
    }

    @Override
    public void start(Stage primaryStage) {
        primaryStage.setTitle("Monitoring tool");

        // Label
        Label label = new Label("Monitoring tool");
        label.setTextFill(Color.DARKBLUE);
        label.setFont(Font.font("Calibri", FontWeight.BOLD, 36));
        HBox hb = new HBox();
        hb.setAlignment(Pos.CENTER);
        hb.getChildren().add(label);

        // Table
        TableView table = new TableView();
        TableColumn taxiIdCol = new TableColumn("Taxi ID");
        taxiIdCol.setCellValueFactory(new PropertyValueFactory<TaxiData, String>("id"));
        TableColumn distanceCol = new TableColumn("Distance (km)");
        distanceCol.setCellValueFactory(new PropertyValueFactory<TaxiData, String>("distance"));
        TableColumn averageSpeedCol = new TableColumn("Average Speed (km/h)");
        averageSpeedCol.setCellValueFactory(new PropertyValueFactory<TaxiData, String>("averageSpeed"));

        table.getColumns().setAll(taxiIdCol, distanceCol, averageSpeedCol);
        table.setPrefWidth(750);
        table.setPrefHeight(600);
        table.setColumnResizePolicy(TableView.CONSTRAINED_RESIZE_POLICY);

        // Vbox
        VBox vbox = new VBox(20);
        vbox.setPadding(new Insets(25, 25, 25, 25));;
        vbox.getChildren().addAll(hb, table);

        // Scene
        primaryStage.setScene(new Scene(vbox, 600, 600));
        primaryStage.show();

        RedisMonitor monitor = new RedisMonitor();

        new Thread(new Runnable() {
            public void run() {
                monitor.run(table);
            }
        }).start();
    }

    public static class TaxiData {

        StringProperty id = new SimpleStringProperty();;
        StringProperty distance = new SimpleStringProperty();;
        StringProperty averageSpeed = new SimpleStringProperty();;

        TaxiData(String id, String distance, String averageSpeed) {
            this.id.set(id);
            this.distance.set(distance);
            this.averageSpeed.set(averageSpeed);
        }

        public String getId() {
            return id.get();
        }

        public StringProperty idProperty() {
            return id;
        }

        public void setId(String id) {
            this.id.set(id);
        }

        public String getDistance() {
            return distance.get();
        }

        public StringProperty distanceProperty() {
            return distance;
        }

        public void setDistance(String distance) {
            this.distance.set(distance);
        }

        public String getAverageSpeed() {
            return averageSpeed.get();
        }

        public StringProperty averageSpeedProperty() {
            return averageSpeed;
        }

        public void setAverageSpeed(String averageSpeed) {
            this.averageSpeed.set(averageSpeed);
        }
    }
}
