package com.jpmc.midascore.component;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Incentive;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

@Component
public class TransactionListener {
    private final UserRepository userRepository;
    private final DatabaseConduit databaseConduit;
    private final TransactionRepository transactionRepository;
    private final RestTemplate restTemplate;

    public TransactionListener(
            UserRepository userRepository,
            DatabaseConduit databaseConduit,
            TransactionRepository transactionRepository,
            RestTemplate restTemplate
    ) {
        this.userRepository = userRepository;
        this.databaseConduit = databaseConduit;
        this.transactionRepository = transactionRepository;
        this.restTemplate = restTemplate;
    }

    @Transactional
    @KafkaListener(topics = "${general.kafka-topic}")
    public void onTransaction(Transaction transaction) {
        if (transaction == null || transaction.getAmount() <= 0) {
            return;
        }

        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        if (sender == null || recipient == null) {
            return;
        }

        if (sender.getBalance() < transaction.getAmount()) {
            return;
        }

        // Call the incentive API
        float incentive = 0;
        try {
            Incentive incentiveResponse = restTemplate.postForObject(
                    "http://localhost:8080/incentive",
                    transaction,
                    Incentive.class
            );
            if (incentiveResponse != null && incentiveResponse.getAmount() >= 0) {
                incentive = incentiveResponse.getAmount();
                transaction.setIncentive(incentive);
            }
        } catch (Exception e) {
            // If API call fails, continue without incentive
            incentive = 0;
        }

        // Update balances
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount() + incentive);

        databaseConduit.save(sender);
        databaseConduit.save(recipient);
        transactionRepository.save(new TransactionRecord(sender, recipient, transaction.getAmount(), incentive));
    }
}

