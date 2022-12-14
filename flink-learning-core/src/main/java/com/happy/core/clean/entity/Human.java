package com.happy.core.clean.entity;

import java.io.Serializable;

/**
 * @author DeveloperZJQ
 * @since 2022-12-14
 */
public class Human implements Serializable{

    private String name;
    private String gender;
    private Integer age;
    private IdentityInfo identityInfo;


    public class IdentityInfo {
        private String idCard;
        private Student.Clazz clazz = new Student().new Clazz();
    }
}
