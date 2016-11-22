package io.github.vdubois.analyzer;

import org.springframework.batch.core.JobParametersInvalidException;
import org.springframework.boot.diagnostics.AbstractFailureAnalyzer;
import org.springframework.boot.diagnostics.FailureAnalysis;

/**
 * Created by vdubois on 22/11/16.
 */
public class JobParametersMissingFailureAnalyzer extends AbstractFailureAnalyzer<JobParametersInvalidException> {

    private static final String ACTION = "Consider addind the following parameters to your job : ";

    private static final String DESCRIPTION = "Some of the mandatory parameters of Spring Batch job are not present";

    @Override
    protected FailureAnalysis analyze(Throwable throwable, JobParametersInvalidException e) {
        return new FailureAnalysis(DESCRIPTION, ACTION + e.getMessage().split("\\[")[1].split("\\]")[0], e);
    }
}
