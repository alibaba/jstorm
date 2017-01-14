/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package backtype.storm.topology;

import java.io.NotSerializableException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json.simple.JSONValue;

import com.alibaba.jstorm.utils.JStormUtils;

import backtype.storm.Config;
import backtype.storm.generated.Bolt;
import backtype.storm.generated.ComponentCommon;
import backtype.storm.generated.ComponentObject;
import backtype.storm.generated.GlobalStreamId;
import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.generated.StateSpoutSpec;
import backtype.storm.generated.StormTopology;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.grouping.PartialKeyGrouping;
import backtype.storm.hooks.IWorkerHook;
import backtype.storm.spout.CheckpointSpout;
import backtype.storm.state.State;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.utils.Utils;
import backtype.storm.windowing.TupleWindow;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Storm to execute. Topologies are Thrift structures in the end, but since the Thrift API is
 * so verbose, TopologyBuilder greatly eases the process of creating topologies. The template for creating and submitting a topology looks something like:
 * 
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 * 
 * builder.setSpout(&quot;1&quot;, new TestWordSpout(true), 5);
 * builder.setSpout(&quot;2&quot;, new TestWordSpout(true), 3);
 * builder.setBolt(&quot;3&quot;, new TestWordCounter(), 3).fieldsGrouping(&quot;1&quot;, new Fields(&quot;word&quot;)).fieldsGrouping(&quot;2&quot;, new Fields(&quot;word&quot;));
 * builder.setBolt(&quot;4&quot;, new TestGlobalCount()).globalGrouping(&quot;1&quot;);
 * 
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * 
 * StormSubmitter.submitTopology(&quot;mytopology&quot;, conf, builder.createTopology());
 * </pre>
 * 
 * Running the exact same topology in local mode (in process), and configuring it to log all tuples emitted, looks like the following. Note that it lets the
 * topology run for 10 seconds before shutting down the local cluster.
 * 
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 * 
 * builder.setSpout(&quot;1&quot;, new TestWordSpout(true), 5);
 * builder.setSpout(&quot;2&quot;, new TestWordSpout(true), 3);
 * builder.setBolt(&quot;3&quot;, new TestWordCounter(), 3).fieldsGrouping(&quot;1&quot;, new Fields(&quot;word&quot;)).fieldsGrouping(&quot;2&quot;, new Fields(&quot;word&quot;));
 * builder.setBolt(&quot;4&quot;, new TestGlobalCount()).globalGrouping(&quot;1&quot;);
 * 
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 * 
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology(&quot;mytopology&quot;, conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 * 
 * <p>
 * The pattern for TopologyBuilder is to map component ids to components using the setSpout and setBolt methods. Those methods return objects that are then used
 * to declare the inputs for that component.
 * </p>
 */
public class TopologyBuilder {
    protected Map<String, IRichBolt> _bolts = new HashMap<>();
    protected Map<String, IRichSpout> _spouts = new HashMap<>();
    protected Map<String, ComponentCommon> _commons = new HashMap<>();
    protected boolean hasStatefulBolt = false;

    // private Map<String, Map<GlobalStreamId, Grouping>> _inputs = new HashMap<String, Map<GlobalStreamId, Grouping>>();

    private Map<String, StateSpoutSpec> _stateSpouts = new HashMap<>();
    private List<ByteBuffer> _workerHooks = new ArrayList<>();

    public StormTopology createTopology() {
        Map<String, Bolt> boltSpecs = new HashMap<>();
        Map<String, SpoutSpec> spoutSpecs = new HashMap<>();
        maybeAddCheckpointSpout();
        for (String boltId : _bolts.keySet()) {
            IRichBolt bolt = _bolts.get(boltId);
            bolt = maybeAddCheckpointTupleForwarder(bolt);
            ComponentCommon common = getComponentCommon(boltId, bolt);
            try{
                maybeAddCheckpointInputs(common);
            boltSpecs.put(boltId, new Bolt(ComponentObject.serialized_java(Utils.javaSerialize(bolt)), common));
            }catch(RuntimeException wrapperCause){
                if (wrapperCause.getCause() != null && NotSerializableException.class.equals(wrapperCause.getCause().getClass())){
                    throw new IllegalStateException(
                        "Bolt '" + boltId + "' contains a non-serializable field of type " + wrapperCause.getCause().getMessage() + ", " +
                        "which was instantiated prior to topology creation. " + wrapperCause.getCause().getMessage() + " " +
                        "should be instantiated within the prepare method of '" + boltId + " at the earliest.", wrapperCause);
                }
                throw wrapperCause;
            }
        }
        for (String spoutId : _spouts.keySet()) {
            IRichSpout spout = _spouts.get(spoutId);
            ComponentCommon common = getComponentCommon(spoutId, spout);
            try{
            spoutSpecs.put(spoutId, new SpoutSpec(ComponentObject.serialized_java(Utils.javaSerialize(spout)), common));
            }catch(RuntimeException wrapperCause){
                if (wrapperCause.getCause() != null && NotSerializableException.class.equals(wrapperCause.getCause().getClass())){
                    throw new IllegalStateException(
                        "Spout '" + spoutId + "' contains a non-serializable field of type " + wrapperCause.getCause().getMessage() + ", " +
                        "which was instantiated prior to topology creation. " + wrapperCause.getCause().getMessage() + " " +
                        "should be instantiated within the prepare method of '" + spoutId + " at the earliest.", wrapperCause);
                }
                throw wrapperCause;
            }
        }

        StormTopology stormTopology = new StormTopology(spoutSpecs,
                boltSpecs,
                new HashMap<String, StateSpoutSpec>());
        //stormTopology.set_worker_hooks(_workerHooks);
        return stormTopology;
    }

    /**
     * Define a new bolt in this topology with parallelism of just one thread.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology with the specified amount of parallelism.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, bolt, parallelism_hint);
        _bolts.put(id, bolt);
        return new BoltGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt) throws IllegalArgumentException {
        return setBolt(id, bolt, null);
    }

    /**
     * Define a new bolt in this topology. This defines a basic bolt, which is a
     * simpler to use but more restricted kind of bolt. Basic bolts are intended
     * for non-aggregation processing and automate the anchoring/acking process to
     * achieve proper reliability in the topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the basic bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        return setBolt(id, new BasicBoltExecutor(bolt), parallelism_hint);
    }

    /**
     * Define a new bolt in this topology. This defines a windowed bolt, intended
     * for windowing operations. The {@link IWindowedBolt#execute(TupleWindow)} method
     * is triggered for each window interval with the list of current events in the window.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the windowed bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
        return setBolt(id, new WindowedBoltExecutor(bolt), parallelism_hint);
    }
    /**
     * Define a new bolt in this topology. This defines a stateful bolt, that requires its
     * state (of computation) to be saved. When this bolt is initialized, the {@link IStatefulBolt#initState(State)} method
     * is invoked after {@link IStatefulBolt#prepare(Map, TopologyContext, OutputCollector)} but before {@link IStatefulBolt#execute(Tuple)}
     * with its previously saved state.
     * <p>
     * The framework provides at-least once guarantee for the state updates. Bolts (both stateful and non-stateful) in a stateful topology
     * are expected to anchor the tuples while emitting and ack the input tuples once its processed.
     * </p>
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the stateful bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulBolt<T> bolt, Number parallelism_hint) throws IllegalArgumentException {
        hasStatefulBolt = true;
        return setBolt(id, new StatefulBoltExecutor<T>(bolt), parallelism_hint);
    }
    /**
     * Define a new bolt in this topology. This defines a stateful windowed bolt, intended for stateful
     * windowing operations. The {@link IStatefulWindowedBolt#execute(TupleWindow)} method is triggered
     * for each window interval with the list of current events in the window. During initialization of
     * this bolt {@link IStatefulWindowedBolt#initState(State)} is invoked with its previously saved state.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the stateful windowed bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
     * @param <T> the type of the state (e.g. {@link org.apache.storm.state.KeyValueState})
     * @return use the returned object to declare the inputs to this component
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public <T extends State> BoltDeclarer setBolt(String id, IStatefulWindowedBolt<T> bolt, Number parallelism_hint) throws IllegalArgumentException {
        hasStatefulBolt = true;
        return setBolt(id, new StatefulBoltExecutor<T>(new StatefulWindowedBoltExecutor<T>(bolt)), parallelism_hint);
    }

    /**
     * Define a new spout in this topology.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout) throws IllegalArgumentException {
        return setSpout(id, spout, null);
    }

    /**
     * Define a new spout in this topology with the specified parallelism. If the spout declares
     * itself as non-distributed, the parallelism_hint will be ignored and only one task
     * will be allocated to this component.
     *
     * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
     * @param parallelism_hint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somewhere around the cluster.
     * @param spout the spout
     * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
     */
    public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);
        initCommon(id, spout, parallelism_hint);
        _spouts.put(id, spout);
        return new SpoutGetter(id);
    }

    /**
     * Define a new bolt in this topology. This defines a control spout, which is a simpler to use but more restricted kind of bolt. Control spouts are intended for
     * making sending control message more simply
     *
     * @param id the id of this component.
     * @param spout the control spout
     */
    public SpoutDeclarer setSpout(String id, IControlSpout spout) {
        return setSpout(id, spout, null);
    }
    public SpoutDeclarer setSpout(String id, IControlSpout spout, Number parallelism_hint) {
        return setSpout(id, new ControlSpoutExecutor(spout), parallelism_hint);
    }
    /**
     * Define a new bolt in this topology. This defines a control bolt, which is a simpler to use but more restricted kind of bolt. Control bolts are intended for
     * making sending control message more simply
     * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
     * @param bolt the control bolt
     * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around
     *            the cluster.
     * @return use the returned object to declare the inputs to this component
     */
    public BoltDeclarer setBolt(String id, IControlBolt bolt, Number parallelism_hint) {
        return setBolt(id, new ControlBoltExecutor(bolt), parallelism_hint);
    }
    public BoltDeclarer setBolt(String id, IControlBolt bolt) {
        return setBolt(id, bolt, null);
    }


    public void setStateSpout(String id, IRichStateSpout stateSpout) throws IllegalArgumentException {
        setStateSpout(id, stateSpout, null);
    }

    public void setStateSpout(String id, IRichStateSpout stateSpout, Number parallelism_hint) throws IllegalArgumentException {
        validateUnusedId(id);
        // TODO: finish
    }

    /**
     * Add a new worker lifecycle hook
     *
     * @param workerHook the lifecycle hook to add
     */
    public void addWorkerHook(IWorkerHook workerHook) {
        if(null == workerHook) {
            throw new IllegalArgumentException("WorkerHook must not be null.");
        }

        _workerHooks.add(ByteBuffer.wrap(Utils.javaSerialize(workerHook)));
    }


    protected void validateUnusedId(String id) {
        if (_bolts.containsKey(id)) {
            throw new IllegalArgumentException("Bolt has already been declared for id " + id);
        }
        if (_spouts.containsKey(id)) {
            throw new IllegalArgumentException("Spout has already been declared for id " + id);
        }
        if (_stateSpouts.containsKey(id)) {
            throw new IllegalArgumentException("State spout has already been declared for id " + id);
        }
    }

    /**
     * If the topology has at least one stateful bolt
     * add a {@link CheckpointSpout} component to the topology.
     */
    private void maybeAddCheckpointSpout() {
        if (hasStatefulBolt) {
            setSpout(CheckpointSpout.CHECKPOINT_COMPONENT_ID, new CheckpointSpout(), 1);
        }
    }

    private void maybeAddCheckpointInputs(ComponentCommon common) {
        if (hasStatefulBolt) {
            addCheckPointInputs(common);
        }
    }

    /**
     * If the topology has at least one stateful bolt all the non-stateful bolts
     * are wrapped in {@link CheckpointTupleForwarder} so that the checkpoint
     * tuples can flow through the topology.
     */
    private IRichBolt maybeAddCheckpointTupleForwarder(IRichBolt bolt) {
        if (hasStatefulBolt && !(bolt instanceof StatefulBoltExecutor)) {
            bolt = new CheckpointTupleForwarder(bolt);
        }
        return bolt;
    }

    /**
     * For bolts that has incoming streams from spouts (the root bolts),
     * add checkpoint stream from checkpoint spout to its input. For other bolts,
     * add checkpoint stream from the previous bolt to its input.
     */
    private void addCheckPointInputs(ComponentCommon component) {
        Set<GlobalStreamId> checkPointInputs = new HashSet<>();
        for (GlobalStreamId inputStream : component.get_inputs().keySet()) {
            String sourceId = inputStream.get_componentId();
            if (_spouts.containsKey(sourceId)) {
                checkPointInputs.add(new GlobalStreamId(CheckpointSpout.CHECKPOINT_COMPONENT_ID, CheckpointSpout.CHECKPOINT_STREAM_ID));
            } else {
                checkPointInputs.add(new GlobalStreamId(sourceId, CheckpointSpout.CHECKPOINT_STREAM_ID));
            }
        }
        for (GlobalStreamId streamId : checkPointInputs) {
            component.put_to_inputs(streamId, Grouping.all(new NullStruct()));
        }
    }
    private ComponentCommon getComponentCommon(String id, IComponent component) {
        ComponentCommon ret = new ComponentCommon(_commons.get(id));

        OutputFieldsGetter getter = new OutputFieldsGetter();
        component.declareOutputFields(getter);
        ret.set_streams(getter.getFieldsDeclaration());
        return ret;
    }

    protected void initCommon(String id, IComponent component, Number parallelism) throws IllegalArgumentException {
        ComponentCommon common = new ComponentCommon();
        common.set_inputs(new HashMap<GlobalStreamId, Grouping>());
        if (parallelism != null) {
            int dop = parallelism.intValue();
            if(dop < 1) {
                throw new IllegalArgumentException("Parallelism must be positive.");
            }
            common.set_parallelism_hint(dop);
        } else {
            common.set_parallelism_hint(1);
        }
        Map conf = component.getComponentConfiguration();
        if(conf!=null) common.set_json_conf(JSONValue.toJSONString(conf));
        _commons.put(id, common);
    }

    protected class ConfigGetter<T extends ComponentConfigurationDeclarer> extends BaseConfigurationDeclarer<T> {
        String _id;

        public ConfigGetter(String id) {
            _id = id;
        }

        @Override
        public T addConfigurations(Map conf) {
            if (conf != null && conf.containsKey(Config.TOPOLOGY_KRYO_REGISTER)) {
                throw new IllegalArgumentException("Cannot set serializations for a component using fluent API");
            }
            String currConf = _commons.get(_id).get_json_conf();
            _commons.get(_id).set_json_conf(JStormUtils.mergeIntoJson(JStormUtils.parseJson(currConf), conf));
            return (T) this;
        }
    }

    protected class SpoutGetter extends ConfigGetter<SpoutDeclarer> implements SpoutDeclarer {
        public SpoutGetter(String id) {
            super(id);
        }
    }

    protected class BoltGetter extends ConfigGetter<BoltDeclarer> implements BoltDeclarer {
        protected String _boltId;

        public BoltGetter(String boltId) {
            super(boltId);
            _boltId = boltId;
        }

        public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
            return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
        }

        public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
            return grouping(componentId, streamId, Grouping.fields(fields.toList()));
        }

        public BoltDeclarer globalGrouping(String componentId) {
            return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer globalGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.fields(new ArrayList<String>()));
        }

        public BoltDeclarer shuffleGrouping(String componentId) {
            return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.shuffle(new NullStruct()));
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId) {
            return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.local_or_shuffle(new NullStruct()));
        }

        @Override
        public BoltDeclarer localFirstGrouping(String componentId) {
            return localFirstGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        @Override
        public BoltDeclarer localFirstGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.localFirst(new NullStruct()));
        }

        public BoltDeclarer noneGrouping(String componentId) {
            return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer noneGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.none(new NullStruct()));
        }

        public BoltDeclarer allGrouping(String componentId) {
            return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer allGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.all(new NullStruct()));
        }

        public BoltDeclarer directGrouping(String componentId) {
            return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
        }

        public BoltDeclarer directGrouping(String componentId, String streamId) {
            return grouping(componentId, streamId, Grouping.direct(new NullStruct()));
        }

        private BoltDeclarer grouping(String componentId, String streamId, Grouping grouping) {
            _commons.get(_boltId).put_to_inputs(new GlobalStreamId(componentId, streamId), grouping);
            return this;
        }

        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
            return customGrouping(componentId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
            return customGrouping(componentId, streamId, new PartialKeyGrouping(fields));
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
            return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
        }

        @Override
        public BoltDeclarer customGrouping(String componentId, String streamId, CustomStreamGrouping grouping) {
            return grouping(componentId, streamId, Grouping.custom_serialized(Utils.javaSerialize(grouping)));
        }

        @Override
        public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
            return grouping(id.get_componentId(), id.get_streamId(), grouping);
        }
    }
}
