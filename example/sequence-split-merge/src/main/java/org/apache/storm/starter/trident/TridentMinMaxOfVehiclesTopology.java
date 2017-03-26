/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter.trident;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.storm.starter.InOrderDeliveryTest.Check;
import org.apache.storm.starter.InOrderDeliveryTest.InOrderSpout;

import com.alibaba.starter.utils.Assert;
import com.alibaba.starter.utils.JStormHelper;

import backtype.storm.Config;
import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.Debug;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

/**
 * This class demonstrates different usages of *
 * {@link Stream#minBy(String, Comparator)} * {@link Stream#min(Comparator)} *
 * {@link Stream#maxBy(String, Comparator)} * {@link Stream#max(Comparator)}
 * operations on trident {@link Stream}.
 */
public class TridentMinMaxOfVehiclesTopology {
    
    /**
     * Creates a topology which demonstrates min/max operations on tuples of
     * stream which contain vehicle and driver fields with values
     * {@link TridentMinMaxOfVehiclesTopology.Vehicle} and
     * {@link TridentMinMaxOfVehiclesTopology.Driver} respectively.
     */
    public static StormTopology buildVehiclesTopology() {
        Fields driverField = new Fields(Driver.FIELD_NAME);
        Fields vehicleField = new Fields(Vehicle.FIELD_NAME);
        Fields allFields = new Fields(Vehicle.FIELD_NAME, Driver.FIELD_NAME);
        
        FixedBatchSpout spout = new FixedBatchSpout(allFields, 10, Vehicle.generateVehicles(20));
        spout.setCycle(true);
        
        TridentTopology topology = new TridentTopology();
        Stream vehiclesStream = topology.newStream("spout1", spout).each(allFields, new Debug("##### vehicles"));
        
        Stream slowVehiclesStream = vehiclesStream.min(new SpeedComparator()).each(vehicleField,
                new Debug("#### slowest vehicle"));
                
        Stream slowDriversStream = slowVehiclesStream.project(driverField).each(driverField,
                new Debug("##### slowest driver"));
                
        vehiclesStream.max(new SpeedComparator()).each(vehicleField, new Debug("#### fastest vehicle"))
                .project(driverField).each(driverField, new Debug("##### fastest driver"));
                
        vehiclesStream.minBy(Vehicle.FIELD_NAME, new EfficiencyComparator()).each(vehicleField,
                new Debug("#### least efficient vehicle"));
                
        vehiclesStream.maxBy(Vehicle.FIELD_NAME, new EfficiencyComparator()).each(vehicleField,
                new Debug("#### most efficient vehicle"));
                
        return topology.build();
    }
    
    static class SpeedComparator implements Comparator<TridentTuple>, Serializable {
        
        @Override
        public int compare(TridentTuple tuple1, TridentTuple tuple2) {
            Vehicle vehicle1 = (Vehicle) tuple1.getValueByField(Vehicle.FIELD_NAME);
            Vehicle vehicle2 = (Vehicle) tuple2.getValueByField(Vehicle.FIELD_NAME);
            return Integer.compare(vehicle1.maxSpeed, vehicle2.maxSpeed);
        }
    }
    
    static class EfficiencyComparator implements Comparator<Vehicle>, Serializable {
        
        @Override
        public int compare(Vehicle vehicle1, Vehicle vehicle2) {
            return Double.compare(vehicle1.efficiency, vehicle2.efficiency);
        }
        
    }
    
    static class Driver implements Serializable {
        static final String FIELD_NAME = "driver";
        final String        name;
        final int           id;
        
        Driver(String name, int id) {
            this.name = name;
            this.id = id;
        }
        
        @Override
        public String toString() {
            return "Driver{" + "name='" + name + '\'' + ", id=" + id + '}';
        }
    }
    
    static class Vehicle implements Serializable {
        static final String FIELD_NAME = "vehicle";
        final String        name;
        final int           maxSpeed;
        final double        efficiency;
        
        public Vehicle(String name, int maxSpeed, double efficiency) {
            this.name = name;
            this.maxSpeed = maxSpeed;
            this.efficiency = efficiency;
        }
        
        @Override
        public String toString() {
            return "Vehicle{" + "name='" + name + '\'' + ", maxSpeed=" + maxSpeed + ", efficiency=" + efficiency + '}';
        }
        
        public static List<Object>[] generateVehicles(int count) {
            List<Object>[] vehicles = new List[count];
            for (int i = 0; i < count; i++) {
                int id = i - 1;
                vehicles[i] = (new Values(new Vehicle("Vehicle-" + id, ThreadLocalRandom.current().nextInt(0, 100),
                        ThreadLocalRandom.current().nextDouble(1, 5)), new Driver("Driver-" + id, id)));
            }
            return vehicles;
        }
    }
    
    static boolean   isLocal = true;
    static LocalDRPC drpc    = null;
    static Config    conf    = JStormHelper.getConfig(null);
    
    public static void test() {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("spout", new InOrderSpout(), 8);
        builder.setBolt("count", new Check(), 8).fieldsGrouping("spout", new Fields("c1"));
        
        conf.setMaxSpoutPending(20);
        
        String[] className = Thread.currentThread().getStackTrace()[1].getClassName().split("\\.");
        String topologyName = className[className.length - 1];
        
        if (isLocal) {
            drpc = new LocalDRPC();
        }
        
        try {
            JStormHelper.runTopology(buildVehiclesTopology(), topologyName, conf, 60,
                    new JStormHelper.CheckAckedFail(conf), isLocal);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            Assert.fail("Failed");
        }
    }
    
    public static void main(String[] args) throws Exception {
        conf = JStormHelper.getConfig(args);
        isLocal = false;
        test();
    }
}
