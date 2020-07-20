package com.napster.objectstoragedemo.controller;

import com.napster.objectstoragedemo.bin.Employee;
import com.napster.objectstoragedemo.util.ObjectStoreUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.*;
import com.google.gson.Gson;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.Random;


@RestController
@RequestMapping(path = "/api")
public class IBMObjectStoreController {
    @Value("${spring.objectstore.uploadBucket}")
    private String bucketNameUS;
    @Autowired
    private ObjectStoreUtils objStoreUtils;

    @PostMapping(path = "/store")
    public String storeToIBMCloudObjectStore(@RequestBody Employee employee){
        Gson gson = new Gson();
        String activeMQLogStr = gson.toJson(employee);

        Date genDate = new Date();
        String dateString = new SimpleDateFormat("yyyy-MM-dd").format(genDate);

        LocalDateTime myDateObj = LocalDateTime.now();
        DateTimeFormatter myFormatObj = DateTimeFormatter.ofPattern("ddMMyyyyHHmmssSSS");
        String formattedDate = myDateObj.format(myFormatObj);
        Random rand = new Random();

        String accessKey = "Test" + "_" + rand.nextInt(1000000) + "_" + formattedDate;
        String filename = dateString + "-" + accessKey + ".json";
        String objStoreResponse = objStoreUtils.storeJsonString(activeMQLogStr, filename, bucketNameUS);
        return objStoreResponse;
    }

    @GetMapping(value = "/getData/{fileName}")
    public String getObjectStoreData(@PathVariable(value = "fileName") final String fileName) throws Exception {
        return objStoreUtils.getCloudObjectStoreFile(fileName,bucketNameUS);
    }

    @GetMapping(value = "/list/{bucketname}")
    public List<String> getBucketObjectList(@PathVariable(value = "bucketname") final String bucketname){
        return objStoreUtils.listObjects(bucketname);
    }
}
