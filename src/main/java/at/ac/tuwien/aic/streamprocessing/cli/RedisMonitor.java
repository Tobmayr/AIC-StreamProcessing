package at.ac.tuwien.aic.streamprocessing.cli;

import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

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

public class RedisMonitor extends Application {
    private static final DecimalFormat DECIMAL_FORMAT = new DecimalFormat("#.##");
    private static boolean isMonitorForOptimizedTopology = false;
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
            List<TaxiData> taxis = getTaxiIds().stream().map(this::queryTaxi).collect(Collectors.toList());
            if (!taxis.isEmpty()) {
                printOverallDistance(taxis);
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

    private void printOverallDistance(List<TaxiData> taxis) {
        Double distance = 0D;
        for (TaxiData data : taxis) {
            distance += Double.valueOf(data.distance.getValue());
        }
        System.out.println("Aggregated distance: " + distance);

    }

    private List<Integer> getTaxiIds() {
        Set<Integer> ids = new HashSet<>();

        for (String key : jedis.keys("*")) {
            Integer id = null;
            if (isKeyForOptimizedTopology(key)) {
                id = Integer.parseInt(key.split(":")[2]);
            } else if (isKeyForDefaultTopology(key)) {
                id = Integer.parseInt(key.split("_")[0]);

            }

            if (id != null) {
                ids.add(id);
            }

        }

        List<Integer> sortedIds = new ArrayList<>(ids);
        Collections.sort(sortedIds);

        return sortedIds;
    }

    private boolean isKeyForOptimizedTopology(String key) {
        return isMonitorForOptimizedTopology && (key.startsWith("tridentState:distance:") || key.startsWith("tridentState:avgSpeed:"));

    }

    private boolean isKeyForDefaultTopology(String key) {
        return !isMonitorForOptimizedTopology && (key.endsWith(InfoType.DISTANCE.getKeyPrefix()) || key.endsWith(InfoType.AVERAGE_SPEED.getKeyPrefix()));
    }

    private TaxiData queryTaxi(Integer id) {
        String distance, averageSpeed;
        if (isMonitorForOptimizedTopology) {

            distance = jedis.get("tridentState:distance:" + id).split(",")[2];
            String avgSpeedState = jedis.get("tridentState:avgSpeed:" + id);
            averageSpeed = Double.toString(Double.valueOf(avgSpeedState.split(",")[1]) / Integer.valueOf(avgSpeedState.split(",")[0]));

        } else {
            distance = jedis.get(id.toString() + InfoType.DISTANCE.getKeyPrefix());
            averageSpeed = jedis.get(id.toString() + InfoType.AVERAGE_SPEED.getKeyPrefix());
        }

        return new TaxiData(id.toString(), round(distance), round(averageSpeed));
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
        isMonitorForOptimizedTopology = (args.length == 1 && args[0].equals("true"));
        System.out.println(isMonitorForOptimizedTopology);
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
        vbox.setPadding(new Insets(25, 25, 25, 25));
        ;
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
