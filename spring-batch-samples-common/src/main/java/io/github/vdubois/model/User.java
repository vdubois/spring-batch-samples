package io.github.vdubois.model;

import lombok.Data;

/**
 * Created by vdubois on 21/11/16.
 */
@Data
public class User extends Resource {

    private String id;

    private String fullName;

    private String position;

    private String companyNumber;
}