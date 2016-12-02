package io.github.vdubois.tasklet;

import io.github.vdubois.service.MyComplexService;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.stereotype.Component;

/**
 * Created by vdubois on 02/12/16.
 */
@Component
public class CalculatePositionTasklet implements Tasklet {

    private MyComplexService service;

    public CalculatePositionTasklet(MyComplexService service) {
        this.service = service;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        String computedValue = service.findMyComplexValue();
        ExecutionContext jobExecutionContext =
                chunkContext.getStepContext().getStepExecution().getJobExecution().getExecutionContext();
        jobExecutionContext.putString("position", computedValue);
        return RepeatStatus.FINISHED;
    }
}
