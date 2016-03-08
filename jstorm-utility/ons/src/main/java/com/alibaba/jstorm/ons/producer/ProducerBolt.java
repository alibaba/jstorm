package com.alibaba.jstorm.ons.producer;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import com.alibaba.jstorm.ons.OnsTuple;
import com.alibaba.jstorm.utils.RunCounter;
import com.aliyun.openservices.ons.api.Message;
import com.aliyun.openservices.ons.api.Producer;
import com.aliyun.openservices.ons.api.SendResult;
import org.apache.log4j.Logger;

import java.util.Map;


public class ProducerBolt implements IRichBolt {

    private static final long serialVersionUID = 2495121976857546346L;
    
    private static final Logger LOG              = Logger.getLogger(ProducerBolt.class);

    protected OutputCollector      collector;
    protected ProducerConfig       producerConfig;
    protected Producer             producer;
    protected RunCounter           runCounter;
    
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        this.runCounter = new RunCounter(ProducerBolt.class);
        this.producerConfig = new ProducerConfig(stormConf);
        try {
			this.producer = ProducerFactory.mkInstance(producerConfig);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			throw new RuntimeException(e);
		}
        
    }
    
    public void execute(Tuple tuple) {
        // TODO Auto-generated method stub
        OnsTuple msgTuple = (OnsTuple)tuple.getValue(0);
        long before = System.currentTimeMillis();
        SendResult sendResult = null;
        try {
        	Message msg = new Message(
        			producerConfig.getTopic(),
        			producerConfig.getSubExpress(),
        			//Message Body
        			//�κζ�������ʽ�����ݣ�ONS�����κθ�Ԥ����ҪProducer��ConsumerЭ�̺�һ�µ����л��ͷ����л���ʽ
        			msgTuple.getMessage().getBody());
        	
        	// ���ô�����Ϣ��ҵ��ؼ����ԣ��뾡����ȫ��Ψһ��
        	// �Է��������޷������յ���Ϣ����£���ͨ��ONS Console��ѯ��Ϣ��������
        	// ע�⣺������Ҳ����Ӱ����Ϣ�����շ�
        	if (msgTuple.getMessage().getKey() != null) {
        		msg.setKey(msgTuple.getMessage().getKey());
        	}
        	//������Ϣ��ֻҪ�����쳣���ǳɹ�
        	sendResult = producer.send(msg);
        	
            LOG.info("Success send msg of " + msgTuple.getMessage().getMsgID());
        	runCounter.count(System.currentTimeMillis() - before);
        } catch (Exception e) {
        	LOG.error("Failed to send message, SendResult:" + sendResult + "\n", e);
        	runCounter.count(System.currentTimeMillis() - before);
            collector.fail(tuple);
            return ;
            //throw new FailedException(e);
        }
        
        collector.ack(tuple);
    }
    
    public void cleanup() {
        // TODO Auto-generated method stub
    	ProducerFactory.rmInstance(producerConfig.getProducerId());
    	producer = null;
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // TODO Auto-generated method stub
        
    }
    
    public Map<String, Object> getComponentConfiguration() {
        // TODO Auto-generated method stub
        return null;
    }
}
