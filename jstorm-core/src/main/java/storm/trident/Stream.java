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
package storm.trident;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import backtype.storm.generated.Grouping;
import backtype.storm.generated.NullStruct;
import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.topology.ResourceDeclarer;
import backtype.storm.topology.base.BaseWindowedBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import storm.trident.fluent.ChainedAggregatorDeclarer;
import storm.trident.fluent.GlobalAggregationScheme;
import storm.trident.fluent.GroupedStream;
import storm.trident.fluent.IAggregatableStream;
import storm.trident.operation.Aggregator;
import storm.trident.operation.Assembly;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.Consumer;
import storm.trident.operation.Filter;
import storm.trident.operation.FlatMapFunction;
import storm.trident.operation.Function;
import storm.trident.operation.MapFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.builtin.ComparisonAggregator;
import storm.trident.operation.builtin.Max;
import storm.trident.operation.builtin.MaxWithComparator;
import storm.trident.operation.builtin.Min;
import storm.trident.operation.builtin.MinWithComparator;
import storm.trident.operation.impl.CombinerAggStateUpdater;
import storm.trident.operation.impl.ConsumerExecutor;
import storm.trident.operation.impl.FilterExecutor;
import storm.trident.operation.impl.FlatMapFunctionExecutor;
import storm.trident.operation.impl.GlobalBatchToPartition;
import storm.trident.operation.impl.IndexHashBatchToPartition;
import storm.trident.operation.impl.MapFunctionExecutor;
import storm.trident.operation.impl.ReducerAggStateUpdater;
import storm.trident.operation.impl.SingleEmitAggregator.BatchToPartition;
import storm.trident.operation.impl.TrueFilter;
import storm.trident.partition.GlobalGrouping;
import storm.trident.partition.IdentityGrouping;
import storm.trident.partition.IndexHashGrouping;
import storm.trident.planner.Node;
import storm.trident.planner.NodeStateInfo;
import storm.trident.planner.PartitionNode;
import storm.trident.planner.ProcessorNode;
import storm.trident.planner.processor.AggregateProcessor;
import storm.trident.planner.processor.EachProcessor;
import storm.trident.planner.processor.MapProcessor;
import storm.trident.planner.processor.PartitionPersistProcessor;
import storm.trident.planner.processor.ProjectedProcessor;
import storm.trident.planner.processor.StateQueryProcessor;
import storm.trident.state.QueryFunction;
import storm.trident.state.StateFactory;
import storm.trident.state.StateSpec;
import storm.trident.state.StateUpdater;
import storm.trident.tuple.TridentTuple;
import storm.trident.util.TridentUtils;
import storm.trident.windowing.InMemoryWindowsStoreFactory;
import storm.trident.windowing.WindowTridentProcessor;
import storm.trident.windowing.WindowsStateFactory;
import storm.trident.windowing.WindowsStateUpdater;
import storm.trident.windowing.WindowsStoreFactory;
import storm.trident.windowing.config.SlidingCountWindow;
import storm.trident.windowing.config.SlidingDurationWindow;
import storm.trident.windowing.config.TumblingCountWindow;
import storm.trident.windowing.config.TumblingDurationWindow;
import storm.trident.windowing.config.WindowConfig;

// TODO: need to be able to replace existing fields with the function fields (like Cascading Fields.REPLACE)
public class Stream implements IAggregatableStream, ResourceDeclarer<Stream> {
    final Node _node;
    final String _name;
    private final TridentTopology _topology;

    protected Stream(TridentTopology topology, String name, Node node) {
        _topology = topology;
        _node = node;
        _name = name;
    }

    /**
     * Applies a label to the stream. Naming a stream will append the label to the name of the bolt(s) created by
     * Trident and will be visible in the Storm UI.
     *
     * @param name - The label to apply to the stream
     * @return
     */
    public Stream name(String name) {
        return new Stream(_topology, name, _node);
    }

    /**
     * Applies a parallelism hint to a stream.
     *
     * @param hint
     * @return
     */
    public Stream parallelismHint(int hint) {
        _node.parallelismHint = hint;
        return this;
    }

    /**
     * Sets the CPU Load resource for the current operation
     */
    @Override
    public Stream setCPULoad(Number load) {
        _node.setCPULoad(load);
        return this;
    }

    /**
     * Sets the Memory Load resources for the current operation.
     * offHeap becomes default
     */
    @Override
    public Stream setMemoryLoad(Number onHeap) {
        _node.setMemoryLoad(onHeap);
        return this;
    }

    /**
     * Sets the Memory Load resources for the current operation.
     */
    @Override
    public Stream setMemoryLoad(Number onHeap, Number offHeap) {
        _node.setMemoryLoad(onHeap, offHeap);
        return this;
    }

    /**
     * Filters out fields from a stream, resulting in a Stream containing only the fields specified by `keepFields`.
     *
     * For example, if you had a Stream `mystream` containing the fields `["a", "b", "c","d"]`, calling"
     *
     * ```java
     * mystream.project(new Fields("b", "d"))
     * ```
     *
     * would produce a stream containing only the fields `["b", "d"]`.
     *
     *
     * @param keepFields The fields in the Stream to keep
     * @return
     */
    public Stream project(Fields keepFields) {
        projectionValidation(keepFields);
        return _topology.addSourcedNode(this, new ProcessorNode(_topology.getUniqueStreamId(), _name, keepFields, new Fields(), new ProjectedProcessor(keepFields)));
    }

    /**
     * ## Grouping Operation
     *
     * @param fields
     * @return
     */
    public GroupedStream groupBy(Fields fields) {
        projectionValidation(fields);
        return new GroupedStream(this, fields);
    }

    /**
     * ## Repartitioning Operation
     *
     * @param fields
     * @return
     */
    public Stream partitionBy(Fields fields) {
        projectionValidation(fields);
        return partition(Grouping.fields(fields.toList()));
    }

    /**
     * ## Repartitioning Operation
     *
     * @param partitioner
     * @return
     */
    public Stream partition(CustomStreamGrouping partitioner) {
        return partition(Grouping.custom_serialized(Utils.javaSerialize(partitioner)));
    }

    /**
     * ## Repartitioning Operation
     *
     * Use random round robin algorithm to evenly redistribute tuples across all target partitions
     *
     * @return
     */
    public Stream shuffle() {
        return partition(Grouping.shuffle(new NullStruct()));
    }

    /**
     * ## Repartitioning Operation
     *
     * Use random round robin algorithm to evenly redistribute tuples across all target partitions, with a preference
     * for local tasks.
     *
     * @return
     */
    public Stream localOrShuffle() {
        return partition(Grouping.local_or_shuffle(new NullStruct()));
    }


    /**
     * ## Repartitioning Operation
     *
     * All tuples are sent to the same partition. The same partition is chosen for all batches in the stream.
     * @return
     */
    public Stream global() {
        // use this instead of storm's built in one so that we can specify a singleemitbatchtopartition
        // without knowledge of storm's internals
        return partition(new GlobalGrouping());
    }

    /**
     * ## Repartitioning Operation
     *
     *  All tuples in the batch are sent to the same partition. Different batches in the stream may go to different
     *  partitions.
     *
     * @return
     */
    public Stream batchGlobal() {
        // the first field is the batch id
        return partition(new IndexHashGrouping(0));
    }

    /**
     * ## Repartitioning Operation
     *
     * Every tuple is replicated to all target partitions. This can useful during DRPC – for example, if you need to do
     * a stateQuery on every partition of data.
     *
     * @return
     */
    public Stream broadcast() {
        return partition(Grouping.all(new NullStruct()));
    }

    /**
     * ## Repartitioning Operation
     *
     * @return
     */
    public Stream identityPartition() {
        return partition(new IdentityGrouping());
    }

    /**
     * ## Repartitioning Operation
     *
     * This method takes in a custom partitioning function that implements
     * {@link org.apache.storm.grouping.CustomStreamGrouping}
     *
     * @param grouping
     * @return
     */
    public Stream partition(Grouping grouping) {
        if (_node instanceof PartitionNode) {
            return each(new Fields(), new TrueFilter()).partition(grouping);
        } else {
            return _topology.addSourcedNode(this, new PartitionNode(_node.streamId, _name, getOutputFields(), grouping));
        }
    }

    /**
     * Applies an `Assembly` to this `Stream`.
     *
     * @see org.apache.storm.trident.operation.Assembly
     * @param assembly
     * @return
     */
    public Stream applyAssembly(Assembly assembly) {
        return assembly.apply(this);
    }

    @Override
    public Stream each(Fields inputFields, Function function, Fields functionFields) {
        projectionValidation(inputFields);
        return _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                        _name,
                        TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                        functionFields,
                        new EachProcessor(inputFields, function)));
    }
    //creates brand new tuples with brand new fields
    @Override
    public Stream partitionAggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    functionFields,
                    functionFields,
                    new AggregateProcessor(inputFields, agg)));
    }

    public Stream stateQuery(TridentState state, Fields inputFields, QueryFunction function, Fields functionFields) {
        projectionValidation(inputFields);
        String stateId = state._node.stateInfo.id;
        Node n = new ProcessorNode(_topology.getUniqueStreamId(),
                        _name,
                        TridentUtils.fieldsConcat(getOutputFields(), functionFields),
                        functionFields,
                        new StateQueryProcessor(stateId, inputFields, function));
        _topology._colocate.get(stateId).add(n);
        return _topology.addSourcedNode(this, n);
    }

    public TridentState partitionPersist(StateFactory stateFactory, Fields inputFields, StateUpdater updater, Fields functionFields) {
      return partitionPersist(new StateSpec(stateFactory), inputFields, updater, functionFields);
    }

    public TridentState partitionPersist(StateSpec stateSpec, Fields inputFields, StateUpdater updater, Fields functionFields) {
        projectionValidation(inputFields);
        String id = _topology.getUniqueStateId();
        ProcessorNode n = new ProcessorNode(_topology.getUniqueStreamId(),
                    _name,
                    functionFields,
                    functionFields,
                    new PartitionPersistProcessor(id, inputFields, updater));
        n.committer = true;
        n.stateInfo = new NodeStateInfo(id, stateSpec);
        return _topology.addSourcedStateNode(this, n);
    }

    public TridentState partitionPersist(StateFactory stateFactory, Fields inputFields, StateUpdater updater) {
        return partitionPersist(stateFactory, inputFields, updater, new Fields());
    }

    public TridentState partitionPersist(StateSpec stateSpec, Fields inputFields, StateUpdater updater) {
        return partitionPersist(stateSpec, inputFields, updater, new Fields());
    }

    public Stream each(Function function, Fields functionFields) {
        return each(null, function, functionFields);
    }

    public Stream each(Fields inputFields, Filter filter) {
        return each(inputFields, new FilterExecutor(filter), new Fields());
    }

    /**
     * Returns a stream consisting of the elements of this stream that match the given filter.
     *
     * @param filter the filter to apply to each trident tuple to determine if it should be included.
     * @return the new stream
     */
    public Stream filter(Filter filter) {
        return each(getOutputFields(), filter);
    }

    /**
     * Returns a stream consisting of the elements of this stream that match the given filter.
     *
     * @param inputFields the fields of the input trident tuple to be selected.
     * @param filter      the filter to apply to each trident tuple to determine if it should be included.
     * @return the new stream
     */
    public Stream filter(Fields inputFields, Filter filter) {
        return each(inputFields, filter);
    }

    /**
     * Returns a stream consisting of the result of applying the given mapping function to the values of this stream.
     *
     * @param function a mapping function to be applied to each value in this stream.
     * @return the new stream
     */
    public Stream map(MapFunction function) {
        projectionValidation(getOutputFields());
        return _topology.addSourcedNode(this,
                                        new ProcessorNode(
                                                _topology.getUniqueStreamId(),
                                                _name,
                                                getOutputFields(),
                                                getOutputFields(),
                                                new MapProcessor(getOutputFields(), new MapFunctionExecutor(function))));
    }

    /**
     * Returns a stream consisting of the results of replacing each value of this stream with the contents
     * produced by applying the provided mapping function to each value. This has the effect of applying
     * a one-to-many transformation to the values of the stream, and then flattening the resulting elements into a new stream.
     *
     * @param function a mapping function to be applied to each value in this stream which produces new values.
     * @return the new stream
     */
    public Stream flatMap(FlatMapFunction function) {
        projectionValidation(getOutputFields());
        return _topology.addSourcedNode(this,
                                        new ProcessorNode(
                                                _topology.getUniqueStreamId(),
                                                _name,
                                                getOutputFields(),
                                                getOutputFields(),
                                                new MapProcessor(getOutputFields(), new FlatMapFunctionExecutor(function))));
    }

    /**
     * Returns a stream consisting of the trident tuples of this stream, additionally performing the provided action on
     * each trident tuple as they are consumed from the resulting stream. This is mostly useful for debugging
     * to see the tuples as they flow past a certain point in a pipeline.
     *
     * @param action the action to perform on the trident tuple as they are consumed from the stream
     * @return the new stream
     */
    public Stream peek(Consumer action) {
        projectionValidation(getOutputFields());
        return _topology.addSourcedNode(this,
                                        new ProcessorNode(
                                                _topology.getUniqueStreamId(),
                                                _name,
                                                getOutputFields(),
                                                getOutputFields(),
                                                new MapProcessor(getOutputFields(), new ConsumerExecutor(action))));
    }

    public ChainedAggregatorDeclarer chainedAgg() {
        return new ChainedAggregatorDeclarer(this, new BatchGlobalAggScheme());
    }

    public Stream partitionAggregate(Aggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(CombinerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .partitionAggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    public Stream partitionAggregate(ReducerAggregator agg, Fields functionFields) {
        return partitionAggregate(null, agg, functionFields);
    }

    public Stream partitionAggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .partitionAggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    /**
     * This aggregator operation computes the minimum of tuples by the given {@code inputFieldName} and it is
     * assumed that its value is an instance of {@code Comparable}. If the value of tuple with field {@code inputFieldName} is not an
     * instance of {@code Comparable} then it throws {@code ClassCastException}
     *
     * @param inputFieldName input field name
     * @return the new stream with this operation.
     */
    public Stream minBy(String inputFieldName) {
        Aggregator<ComparisonAggregator.State> min = new Min(inputFieldName);
        return comparableAggregateStream(inputFieldName, min);
    }

    /**
     * This aggregator operation computes the minimum of tuples by the given {@code inputFieldName} in a stream by
     * using the given {@code comparator}. If the value of tuple with field {@code inputFieldName} is not an
     * instance of {@code T} then it throws {@code ClassCastException}
     *
     * @param inputFieldName input field name
     * @param comparator comparator used in for finding minimum of two tuple values of {@code inputFieldName}.
     * @param <T> type of tuple's given input field value.
     * @return the new stream with this operation.
     */
    public <T> Stream minBy(String inputFieldName, Comparator<T> comparator) {
        Aggregator<ComparisonAggregator.State> min = new MinWithComparator<>(inputFieldName, comparator);
        return comparableAggregateStream(inputFieldName, min);
    }

    /**
     * This aggregator operation computes the minimum of tuples in a stream by using the given {@code comparator} with
     * {@code TridentTuple}s.
     *
     * @param comparator comparator used in for finding minimum of two tuple values.
     * @return the new stream with this operation.
     */
    public Stream min(Comparator<TridentTuple> comparator) {
        Aggregator<ComparisonAggregator.State> min = new MinWithComparator<>(comparator);
        return comparableAggregateStream(null, min);
    }

    /**
     * This aggregator operation computes the maximum of tuples by the given {@code inputFieldName} and it is
     * assumed that its value is an instance of {@code Comparable}. If the value of tuple with field {@code inputFieldName} is not an
     * instance of {@code Comparable} then it throws {@code ClassCastException}
     *
     * @param inputFieldName input field name
     * @return the new stream with this operation.
     */
    public Stream maxBy(String inputFieldName) {
        Aggregator<ComparisonAggregator.State> max = new Max(inputFieldName);
        return comparableAggregateStream(inputFieldName, max);
    }

    /**
     * This aggregator operation computes the maximum of tuples by the given {@code inputFieldName} in a stream by
     * using the given {@code comparator}. If the value of tuple with field {@code inputFieldName} is not an
     * instance of {@code T} then it throws {@code ClassCastException}
     *
     * @param inputFieldName input field name
     * @param comparator comparator used in for finding maximum of two tuple values of {@code inputFieldName}.
     * @param <T> type of tuple's given input field value.
     * @return the new stream with this operation.
     */
    public <T> Stream maxBy(String inputFieldName, Comparator<T> comparator) {
        Aggregator<ComparisonAggregator.State> max = new MaxWithComparator<>(inputFieldName, comparator);
        return comparableAggregateStream(inputFieldName, max);
    }

    /**
     * This aggregator operation computes the maximum of tuples in a stream by using the given {@code comparator} with
     * {@code TridentTuple}s.
     *
     * @param comparator comparator used in for finding maximum of two tuple values.
     * @return the new stream with this operation.
     */
    public Stream max(Comparator<TridentTuple> comparator) {
        Aggregator<ComparisonAggregator.State> max = new MaxWithComparator<>(comparator);
        return comparableAggregateStream(null, max);
    }

    private <T> Stream comparableAggregateStream(String inputFieldName, Aggregator<T> aggregator) {
        if(inputFieldName != null) {
            projectionValidation(new Fields(inputFieldName));
        }
        return partitionAggregate(getOutputFields(), aggregator, getOutputFields());
    }

    public Stream aggregate(Aggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, Aggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .aggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    public Stream aggregate(CombinerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
               .aggregate(inputFields, agg, functionFields)
               .chainEnd();
    }

    public Stream aggregate(ReducerAggregator agg, Fields functionFields) {
        return aggregate(null, agg, functionFields);
    }

    public Stream aggregate(Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return chainedAgg()
                .aggregate(inputFields, agg, functionFields)
                .chainEnd();
    }

    /**
     * Returns a stream of tuples which are aggregated results of a tumbling window with every {@code windowCount} of tuples.
     *
     * @param windowCount represents number of tuples in the window
     * @param windowStoreFactory intermediary tuple store for storing windowing tuples
     * @param inputFields projected fields for aggregator
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream tumblingWindow(int windowCount, WindowsStoreFactory windowStoreFactory,
                                      Fields inputFields, Aggregator aggregator, Fields functionFields) {
        return window(TumblingCountWindow.of(windowCount), windowStoreFactory, inputFields, aggregator, functionFields);
    }

    /**
     * Returns a stream of tuples which are aggregated results of a sliding window with every {@code windowCount} of tuples
     * and slides the window after {@code slideCount}.
     *
     * @param windowCount represents tuples count of a window
     * @param inputFields projected fields for aggregator
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream slidingWindow(int windowCount, int slideCount, WindowsStoreFactory windowStoreFactory,
                                     Fields inputFields, Aggregator aggregator, Fields functionFields) {
        return window(SlidingCountWindow.of(windowCount, slideCount), windowStoreFactory, inputFields, aggregator, functionFields);
    }

    /**
     * Returns a stream of tuples which are aggregated results of a window that tumbles at duration of {@code windowDuration}
     *
     * @param windowDuration represents tumbling window duration configuration
     * @param windowStoreFactory intermediary tuple store for storing windowing tuples
     * @param inputFields projected fields for aggregator
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream tumblingWindow(BaseWindowedBolt.Duration windowDuration, WindowsStoreFactory windowStoreFactory,
                                     Fields inputFields, Aggregator aggregator, Fields functionFields) {
        return window(TumblingDurationWindow.of(windowDuration), windowStoreFactory, inputFields, aggregator, functionFields);
    }

    /**
     * Returns a stream of tuples which are aggregated results of a window which slides at duration of {@code slidingInterval}
     * and completes a window at {@code windowDuration}
     *
     * @param windowDuration represents window duration configuration
     * @param inputFields projected fields for aggregator
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream slidingWindow(BaseWindowedBolt.Duration windowDuration, BaseWindowedBolt.Duration slidingInterval,
                                    WindowsStoreFactory windowStoreFactory, Fields inputFields, Aggregator aggregator, Fields functionFields) {
        return window(SlidingDurationWindow.of(windowDuration, slidingInterval), windowStoreFactory, inputFields, aggregator, functionFields);
    }

    /**
     * Returns a stream of aggregated results based on the given window configuration which uses inmemory windowing tuple store.
     *
     * @param windowConfig window configuration like window length and slide length.
     * @param inputFields input fields
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream window(WindowConfig windowConfig, Fields inputFields, Aggregator aggregator, Fields functionFields) {
        // this store is used only for storing triggered aggregated results but not tuples as storeTuplesInStore is set
        // as false int he below call.
        InMemoryWindowsStoreFactory inMemoryWindowsStoreFactory = new InMemoryWindowsStoreFactory();
        return window(windowConfig, inMemoryWindowsStoreFactory, inputFields, aggregator, functionFields, false);
    }

    /**
     * Returns stream of aggregated results based on the given window configuration.
     *
     * @param windowConfig window configuration like window length and slide length.
     * @param windowStoreFactory intermediary tuple store for storing tuples for windowing
     * @param inputFields input fields
     * @param aggregator aggregator to run on the window of tuples to compute the result and emit to the stream.
     * @param functionFields fields of values to emit with aggregation.
     *
     * @return the new stream with this operation.
     */
    public Stream window(WindowConfig windowConfig, WindowsStoreFactory windowStoreFactory, Fields inputFields,
                         Aggregator aggregator, Fields functionFields) {
        return window(windowConfig, windowStoreFactory, inputFields, aggregator, functionFields, true);
    }

    private Stream window(WindowConfig windowConfig, WindowsStoreFactory windowStoreFactory, Fields inputFields, Aggregator aggregator,
                          Fields functionFields, boolean storeTuplesInStore) {
        projectionValidation(inputFields);
        windowConfig.validate();

        Fields fields = addTriggerField(functionFields);

        // when storeTuplesInStore is false then the given windowStoreFactory is only used to store triggers and
        // that store is passed to WindowStateUpdater to remove them after committing the batch.
        Stream stream = _topology.addSourcedNode(this,
                new ProcessorNode(_topology.getUniqueStreamId(),
                        _name,
                        fields,
                        fields,
                        new WindowTridentProcessor(windowConfig, _topology.getUniqueWindowId(), windowStoreFactory,
                                inputFields, aggregator, storeTuplesInStore)));

        Stream effectiveStream = stream.project(functionFields);

        // create StateUpdater with the given windowStoreFactory to remove triggered aggregation results form store
        // when they are successfully processed.
        StateFactory stateFactory = new WindowsStateFactory();
        StateUpdater stateUpdater = new WindowsStateUpdater(windowStoreFactory);
        stream.partitionPersist(stateFactory, new Fields(WindowTridentProcessor.TRIGGER_FIELD_NAME), stateUpdater, new Fields());

        return effectiveStream;
    }

    private Fields addTriggerField(Fields functionFields) {
        List<String> fieldsList = new ArrayList<>();
        fieldsList.add(WindowTridentProcessor.TRIGGER_FIELD_NAME);
        for (String field : functionFields) {
            fieldsList.add(field);
        }
        return new Fields(fieldsList);
    }

    public TridentState partitionPersist(StateFactory stateFactory, StateUpdater updater, Fields functionFields) {
        return partitionPersist(new StateSpec(stateFactory), updater, functionFields);
    }

    public TridentState partitionPersist(StateSpec stateSpec, StateUpdater updater, Fields functionFields) {
        return partitionPersist(stateSpec, null, updater, functionFields);
    }

    public TridentState partitionPersist(StateFactory stateFactory, StateUpdater updater) {
        return partitionPersist(stateFactory, updater, new Fields());
    }

    public TridentState partitionPersist(StateSpec stateSpec, StateUpdater updater) {
        return partitionPersist(stateSpec, updater, new Fields());
    }

    public TridentState persistentAggregate(StateFactory stateFactory, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, CombinerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        // replaces normal aggregation here with a global grouping because it needs to be consistent across batches 
        return new ChainedAggregatorDeclarer(this, new GlobalAggScheme())
                .aggregate(inputFields, agg, functionFields)
                .chainEnd()
               .partitionPersist(spec, functionFields, new CombinerAggStateUpdater(agg), functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(spec, null, agg, functionFields);
    }

    public TridentState persistentAggregate(StateFactory stateFactory, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        return persistentAggregate(new StateSpec(stateFactory), inputFields, agg, functionFields);
    }

    public TridentState persistentAggregate(StateSpec spec, Fields inputFields, ReducerAggregator agg, Fields functionFields) {
        projectionValidation(inputFields);
        return global().partitionPersist(spec, inputFields, new ReducerAggStateUpdater(agg), functionFields);
    }

    public Stream stateQuery(TridentState state, QueryFunction function, Fields functionFields) {
        return stateQuery(state, null, function, functionFields);
    }

    @Override
    public Stream toStream() {
        return this;
    }

    @Override
    public Fields getOutputFields() {
        return _node.allOutputFields;
    }

    static class BatchGlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.batchGlobal();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new IndexHashBatchToPartition();
        }

    }

    static class GlobalAggScheme implements GlobalAggregationScheme<Stream> {

        @Override
        public IAggregatableStream aggPartition(Stream s) {
            return s.global();
        }

        @Override
        public BatchToPartition singleEmitPartitioner() {
            return new GlobalBatchToPartition();
        }

    }

    private void projectionValidation(Fields projFields) {
        if (projFields == null) {
            return;
        }

        Fields allFields = this.getOutputFields();
        for (String field : projFields) {
            if (!allFields.contains(field)) {
                throw new IllegalArgumentException("Trying to select non-existent field: '" + field + "' from stream containing fields fields: <" + allFields + ">");
            }
        }
    }
}
