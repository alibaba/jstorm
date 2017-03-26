package com.jstorm.example.unittests.trident;

import backtype.storm.Config;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.jstorm.example.unittests.utils.JStormUnitTestRunner;
import org.junit.Test;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Consumer;
import storm.trident.operation.builtin.Debug;
import storm.trident.tuple.TridentTuple;
import static org.junit.Assert.*;
import java.io.Serializable;
import java.util.*;

/**
 * Created by binyang.dby on 2016/7/26.
 *
 * basically the unit test of TridentMinMaxOfVehiclesTopology, I change the origin spout
 * to a spout which can emit 2 fields in a shuffle order of 2 input contents. Here I
 * generate 10 Vehicles and Drivers, the Vehicles has random speed and efficiency. I
 * mark down the max and min values of the speed and efficiency, and assert the value
 * matches the value in the receiving tuple.
 */
public class TridentMinMaxOfVehiclesTest {
    public final static int SPOUT_BATCH_SIZE = 10;

    @Test
    public void testTridentMinMaxOfVehicles()
    {
        Fields driverField = new Fields(Driver.FIELD_NAME);
        Fields vehicleField = new Fields(Vehicle.FIELD_NAME);
        Fields fields = new Fields(Vehicle.FIELD_NAME, Driver.FIELD_NAME);

        Random random = new Random(System.currentTimeMillis());
        List<Values> vehicleContent = new ArrayList<Values>();
        List<Values> driverContent = new ArrayList<Values>();

        int maxSpeed = -1, minSpeed = 10000;
        double maxEfficiency = -1, minEfficiency = 10000;

        for(int i=0; i<SPOUT_BATCH_SIZE; i++)
        {
            int speed = random.nextInt(10000);
            maxSpeed = Math.max(speed, maxSpeed);
            minSpeed = Math.min(speed, minSpeed);

            double efficiency = random.nextDouble() * 10000;
            maxEfficiency = Math.max(efficiency, maxEfficiency);
            minEfficiency = Math.min(efficiency, minEfficiency);

            vehicleContent.add(new Values(new Vehicle("vehicle-" + (i+1), speed, efficiency)));
            driverContent.add(new Values(new Driver("driver-" + (i+1), i+1)));
        }

        ShuffleValuesBatchSpout spout = new ShuffleValuesBatchSpout(fields, vehicleContent, driverContent);

        TridentTopology tridentTopology = new TridentTopology();
        Stream vehiclesStream = tridentTopology.newStream("spout", spout).each(fields, new Debug("#### vehicles"));

        Stream slowVehiclesStream = vehiclesStream.min(new SpeedComparator()).each(vehicleField,
                new Debug("#### slowest vehicle")).peek(new SpeedValidator(minSpeed));

        Stream slowDriversStream = slowVehiclesStream.project(driverField).each(driverField,
                new Debug("#### slowest driver"));

        vehiclesStream.max(new SpeedComparator()).each(vehicleField, new Debug("#### fastest vehicle"))
                .peek(new SpeedValidator(maxSpeed))
                .project(driverField).each(driverField, new Debug("#### fastest driver"));

        vehiclesStream.minBy(Vehicle.FIELD_NAME, new EfficiencyComparator()).each(vehicleField,
                new Debug("#### least efficient vehicle")).peek(new EfficiencyValidator(minEfficiency));

        vehiclesStream.maxBy(Vehicle.FIELD_NAME, new EfficiencyComparator()).each(vehicleField,
                new Debug("#### most efficient vehicle")).peek(new EfficiencyValidator(maxEfficiency));

        Map config = new HashMap();
        config.put(Config.TOPOLOGY_NAME, "TridentMinMaxOfVehiclesTest");

        //use the assert in the body of consumer.accept() to validate
        JStormUnitTestRunner.submitTopology(tridentTopology.build(), null, 120, null);
    }

    private static class SpeedValidator implements Consumer
    {
        private int speed;

        public SpeedValidator(int speed) {
            this.speed = speed;
        }

        @Override
        public void accept(TridentTuple input) {
            Vehicle vehicle = (Vehicle) input.get(0);
            assertEquals(speed, vehicle.getSpeed());
        }
    }

    private static class EfficiencyValidator implements Consumer
    {
        private double efficiency;

        public EfficiencyValidator(double efficiency) {
            this.efficiency = efficiency;
        }

        @Override
        public void accept(TridentTuple input) {
            Vehicle vehicle = (Vehicle) input.get(0);
            assertEquals(efficiency, vehicle.getEfficiency(), 0);
        }
    }

    private static class Driver implements Serializable {
        public final static String FIELD_NAME = "driver";
        private String name;
        private int id;

        public Driver(String name, int id) {
            this.name = name;
            this.id = id;
        }

        @Override
        public String toString() {
            return "Driver{" +
                    "name='" + name + '\'' +
                    ", id=" + id +
                    '}';
        }
    }

    private static class Vehicle implements Serializable {
        public final static String FIELD_NAME = "vehicle";
        private String name;
        private int speed;
        private double efficiency;

        public Vehicle(String name, int maxSpeed, double efficiency) {
            this.name = name;
            this.speed = maxSpeed;
            this.efficiency = efficiency;
        }

        public int getSpeed() {
            return speed;
        }

        public double getEfficiency() {
            return efficiency;
        }

        @Override
        public String toString() {
            return "Vehicle{" +
                    "name='" + name + '\'' +
                    ", maxSpeed=" + speed +
                    ", efficiency=" + efficiency +
                    '}';
        }
    }

    private static class SpeedComparator implements Comparator<TridentTuple>, Serializable {
        @Override
        public int compare(TridentTuple tuple1, TridentTuple tuple2) {
            Vehicle vehicle1 = (Vehicle) tuple1.getValueByField(Vehicle.FIELD_NAME);
            Vehicle vehicle2 = (Vehicle) tuple2.getValueByField(Vehicle.FIELD_NAME);
            return Integer.compare(vehicle1.speed, vehicle2.speed);
        }
    }

    private static class EfficiencyComparator implements Comparator<Vehicle>, Serializable {
        @Override
        public int compare(Vehicle vehicle1, Vehicle vehicle2) {
            return Double.compare(vehicle1.efficiency, vehicle2.efficiency);
        }
    }
}
