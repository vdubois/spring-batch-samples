package io.github.vdubois.model;

import lombok.Data;

/**
 * Created by vdubois on 23/11/16.
 */
@Data
public class MailingList extends Resource {

    private String id;

    private String email;
}
