package com.example;

import com.example.entity.WikimediaData;
import com.example.repository.WikimediaDataRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaDataBaseConsumer {

    @Autowired
    private WikimediaDataRepository wikimediaDataRepository;


    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaDataBaseConsumer.class);

    @KafkaListener(
            topics = "wikimedia_recentchange",
            groupId = "myGroup"
    )
    public void consume(String eventMessage){
        LOGGER.info("Event Message received -> {}",eventMessage);
        WikimediaData wikimediaData = new WikimediaData();
        wikimediaData.setEventData(eventMessage);
        wikimediaDataRepository.save(wikimediaData);

    }
}
