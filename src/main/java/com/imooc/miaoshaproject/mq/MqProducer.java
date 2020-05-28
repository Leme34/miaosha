package com.imooc.miaoshaproject.mq;

import com.alibaba.fastjson.JSON;
import com.imooc.miaoshaproject.dao.StockLogDOMapper;
import com.imooc.miaoshaproject.dataobject.StockLogDO;
import com.imooc.miaoshaproject.error.BusinessException;
import com.imooc.miaoshaproject.service.OrderService;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.*;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hzllb on 2019/2/23.
 */
@Component
public class MqProducer {

    private DefaultMQProducer producer;

    private TransactionMQProducer transactionMQProducer;

    @Value("${mq.nameserver.addr}")
    private String nameAddr;

    @Value("${mq.topicname}")
    private String topicName;


    @Autowired
    private OrderService orderService;

    @Autowired
    private StockLogDOMapper stockLogDOMapper;

    @PostConstruct
    public void init() throws MQClientException {
        // 初始化并启动 mq producer
        producer = new DefaultMQProducer("producer_group");
        producer.setNamesrvAddr(nameAddr);
        producer.start();

        // 初始化并启动带有事务机制的 mq producer
        transactionMQProducer = new TransactionMQProducer("transaction_producer_group");
        transactionMQProducer.setNamesrvAddr(nameAddr);
        transactionMQProducer.start();
        // 事务机制处理监听器
        transactionMQProducer.setTransactionListener(new TransactionListener() {
            /**
             * 当使用TransactionMQProducer.sendMessageInTransaction方法投递消息时，是不会直接被消费者拉取到的
             * 而是会被此处的TransactionListener.executeLocalTransaction方法处理，类似于二段提交，
             * 因此我们可以在此处执行下单逻辑，若下单流程抛出异常则回滚掉数据库扣减库存的消息
             *
             * @param msg 消息体
             * @param arg 投递消息时携带的参数
             * @return 事务型消息状态
             *         若返回 LocalTransactionState.COMMIT_MESSAGE 则真正提交消息（会被消费者消费）；
             *         若返回 LocalTransactionState.ROLLBACK_MESSAGE 则回滚消息（不会被消费者消费）；
             */
            @Override
            public LocalTransactionState executeLocalTransaction(Message msg, Object arg) {
                //取出传递的参数，并创建订单
                Map<Object, Object> argsMap = (Map) arg;
                Integer itemId = (Integer) argsMap.get("itemId");
                Integer promoId = (Integer) argsMap.get("promoId");
                Integer userId = (Integer) argsMap.get("userId");
                Integer amount = (Integer) argsMap.get("amount");
                String stockLogId = (String) argsMap.get("stockLogId");
                try {
                    orderService.createOrder(userId, itemId, promoId, amount, stockLogId);
                } catch (BusinessException e) {  //下单逻辑抛出异常，设置订单对应的stockLog为回滚状态
                    e.printStackTrace();
                    StockLogDO stockLogDO = stockLogDOMapper.selectByPrimaryKey(stockLogId);
                    stockLogDO.setStatus(3);
                    stockLogDOMapper.updateByPrimaryKeySelective(stockLogDO);
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
                // 标记为可提交消息，可以被消费者消费
                return LocalTransactionState.COMMIT_MESSAGE;
            }

            /**
             * 定期回调该方法处理状态为LocalTransactionState.UNKNOW的事务型消息
             *
             * 场景：
             * 当使用TransactionMQProducer.sendMessageInTransaction方法投递消息后
             * 至到TransactionListener.executeLocalTransaction方法处理过程中发生断电等，
             * 都会导致事务型消息状态为LocalTransactionState.UNKNOW
             *
             * @param msg 消息体
             * @return 事务型消息状态
             *         若返回 LocalTransactionState.COMMIT_MESSAGE 则真正提交消息（会被消费者消费）；
             *         若返回 LocalTransactionState.ROLLBACK_MESSAGE 则回滚消息（不会被消费者消费）；
             */
            @Override
            public LocalTransactionState checkLocalTransaction(MessageExt msg) {
                // 根据是否扣减库存成功，来判断要返回COMMIT,ROLLBACK还是继续UNKNOWN
                // 取出库存流水记录判断即可，status = 1：初始状态，2：扣减库存成功，3：扣减失败，下单回滚
                String jsonString = new String(msg.getBody());
                Map<String, Object> map = JSON.parseObject(jsonString, Map.class);
                String stockLogId = (String) map.get("stockLogId");
                StockLogDO stockLogDO = stockLogDOMapper.selectByPrimaryKey(stockLogId);
                if (stockLogDO == null || stockLogDO.getStatus() == 1) {
                    return LocalTransactionState.UNKNOW;
                } else if (stockLogDO.getStatus() == 2) {
                    return LocalTransactionState.COMMIT_MESSAGE;
                } else {
                    return LocalTransactionState.ROLLBACK_MESSAGE;
                }
            }
        });
    }

    /**
     * 事务型下单消息
     *
     * @param userId
     * @param itemId
     * @param promoId
     * @param amount
     * @param stockLogId
     * @return 是否成功投递数据库库存扣减消息
     */
    public boolean transactionAsyncCreateOrder(Integer userId, Integer itemId, Integer promoId, Integer amount, String stockLogId) {
        // 消息体
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("itemId", itemId);
        bodyMap.put("amount", amount);
        bodyMap.put("stockLogId", stockLogId);
        // 参数体
        Map<String, Object> argsMap = new HashMap<>();
        argsMap.put("itemId", itemId);
        argsMap.put("amount", amount);
        argsMap.put("userId", userId);
        argsMap.put("promoId", promoId);
        argsMap.put("stockLogId", stockLogId);

        TransactionSendResult sendResult;
        Message message = new Message(topicName, "increase",
                JSON.toJSON(bodyMap).toString().getBytes(StandardCharsets.UTF_8));
        try {
            // 使用带有事务机制的 mq producer投递消息，类似于二阶段提交（返回消息投递结果）
            sendResult = transactionMQProducer.sendMessageInTransaction(message, argsMap);
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        }
        // 当且仅当消息为提交状态才算投递成功
        return sendResult.getLocalTransactionState() == LocalTransactionState.COMMIT_MESSAGE;
    }

    /**
     * 异步库存扣减消息
     *
     * @param itemId
     * @param amount
     * @return 扣减库存成功与否
     */
    public boolean asyncReduceStock(Integer itemId, Integer amount) {
        // 封装为json消息体
        Map<String, Object> bodyMap = new HashMap<>();
        bodyMap.put("itemId", itemId);
        bodyMap.put("amount", amount);
        Message message = new Message(topicName, "increase",
                JSON.toJSON(bodyMap).toString().getBytes(StandardCharsets.UTF_8));
        // 发送消息
        try {
            producer.send(message);
        } catch (MQClientException e) {
            e.printStackTrace();
            return false;
        } catch (RemotingException e) {
            e.printStackTrace();
            return false;
        } catch (MQBrokerException e) {
            e.printStackTrace();
            return false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }
}
