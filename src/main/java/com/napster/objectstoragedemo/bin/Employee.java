package com.napster.objectstoragedemo.bin;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
@Data
@Getter
@Setter
public class Employee implements Serializable {
    private String name;
    private String address;
}
