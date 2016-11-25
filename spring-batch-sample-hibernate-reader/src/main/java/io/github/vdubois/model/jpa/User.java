package io.github.vdubois.model.jpa;

import lombok.Data;

import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;

/**
 * Created by vdubois on 25/11/16.
 */
@Entity
@Table(name = "users")
@Data
public class User {

    @Id
    private String id;

    private String name;

    private String position;

    private String companyNumber;
}

