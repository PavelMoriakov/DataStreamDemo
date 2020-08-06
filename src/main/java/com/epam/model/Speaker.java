package com.epam.model;

import lombok.Data;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

@Data
public class Speaker {

    @JsonProperty
    private String name;

    @JsonProperty
    private int age;

    @JsonProperty
    private List<String> keywords;

}
