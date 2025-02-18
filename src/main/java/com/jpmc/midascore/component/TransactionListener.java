package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.repository.UserRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.h2.engine.Database;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import com.jpmc.midascore.foundation.Transaction;

@Component
public class TransactionListener {

    UserRepository userRepository;

    DatabaseConduit databaseConduit;

    IncentiveManager incentiveManager;

    private static final Logger logger = LoggerFactory.getLogger(TransactionListener.class);

    public TransactionListener(UserRepository userRepository, DatabaseConduit databaseConduit, IncentiveManager incentiveManager) {
        this.userRepository = userRepository;
        this.databaseConduit = databaseConduit;
        this.incentiveManager = incentiveManager;
    }

    @KafkaListener(topics = "${general.kafka-topic}")
    public void listen(ConsumerRecord<String, Transaction> record) {

        Transaction transaction = record.value();
        long senderId = transaction.getSenderId();
        long recipientId = transaction.getRecipientId();
        float amount = transaction.getAmount();

        UserRecord sender = userRepository.findById(senderId);
        UserRecord recipient = userRepository.findById(recipientId);

        if (sender != null && recipient != null) {
            if (amount <= sender.getBalance()) {
                databaseConduit.save(transaction, sender, recipient);
                sender.setBalance(sender.getBalance() - amount);
                recipient.setBalance(recipient.getBalance() + amount + incentiveManager.query(transaction).getAmount());
                databaseConduit.save(sender);
                databaseConduit.save(recipient);

                if (sender.getName().equals("wilbur")) {
                    logger.info(sender.getName() + " has balance:" + sender.getBalance());
                } else if (recipient.getName().equals("wilbur")) {
                    logger.info(recipient.getName() + " has balance:" + recipient.getBalance());
                }
            }
        }

        //logger.info("Received transaction: {}", transaction);
    }
}