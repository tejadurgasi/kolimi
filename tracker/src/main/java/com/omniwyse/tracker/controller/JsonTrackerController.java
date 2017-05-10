package com.omniwyse.tracker.controller;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omniwyse.tracker.service.JsonProducer;


@RestController
public class JsonTrackerController {

    @Autowired
    private JsonProducer kafkaJson;

    @RequestMapping("/")
    String home() {
        return "Welcome to Tracker application!";
    }

    @RequestMapping(path = "/track", method = RequestMethod.POST, headers = "content-type=application/json")
    String track(@RequestBody Message message) throws IOException, ParseException {
        ObjectMapper mapper = new ObjectMapper();
        // Object to JSON in file
        String str = mapper.writeValueAsString(message);
        // Object to JSON in String
        String tenantid = message.getId();
        String schemaid = message.getType();
        
        JSONParser parser = new JSONParser();
        JSONArray obj = (JSONArray) parser.parse(new InputStreamReader(getClass().getClassLoader().getResourceAsStream("Tenant.json")));
        for (Object o : obj) {
            JSONObject jsonObject = (JSONObject) o;
            String name = (String) jsonObject.get("id");
            JSONArray companyList = (JSONArray) jsonObject.get("schema");
            System.out.println("id: " + name);
            if (name.equals(tenantid)) {
                System.out.println("Tenant Found for " + name);
                Iterator<String> iterator = companyList.iterator();
                while (iterator.hasNext()) {
                    String jsonSchema = iterator.next();
                    if (jsonSchema.equals(schemaid)) {
                        System.out.println("Schema Found for " + jsonSchema);
                        kafkaJson.send(str);
                        return "acknowledged user info";
                    }
                }
            }
        }
        return "Invalid Tenant or Schema";
    }
}