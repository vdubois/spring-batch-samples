package io.github.vdubois.tasklet;

import org.apache.commons.io.IOUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Enumeration;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * Created by vdubois on 22/11/16.
 */
public class DecompressTasklet implements Tasklet {

    private Resource inputFile;

    private String targetDirectory;

    private String targetFile;

    /**
     * Given the current context in the form of a step contribution, do whatever
     * is necessary to process this unit inside a transaction. Implementations
     * return {@link org.springframework.batch.repeat.RepeatStatus#FINISHED} if finished. If not they return
     * {@link org.springframework.batch.repeat.RepeatStatus#CONTINUABLE}. On failure throws an exception.
     *
     * @param contribution mutable state to be passed back to update the current
     *                     step execution
     * @param chunkContext attributes shared between invocations but not between
     *                     restarts
     * @return an {@link org.springframework.batch.repeat.RepeatStatus} indicating whether processing is
     * continuable.
     */
    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        ZipFile zipFile = new ZipFile(inputFile.getFile());
        Enumeration<? extends ZipEntry> entries = zipFile.entries();
        while (entries.hasMoreElements()) {
            ZipEntry entry = entries.nextElement();
            File entryDestination = new File(targetDirectory, targetFile);
            entryDestination.getParentFile().mkdirs();
            InputStream in = zipFile.getInputStream(entry);
            OutputStream out = new FileOutputStream(entryDestination);
            IOUtils.copy(in, out);
            IOUtils.closeQuietly(in);
            IOUtils.closeQuietly(out);
        }
        zipFile.close();
        if (!new File(targetDirectory, targetFile).exists()) {
            throw new IllegalStateException("Unable to decompress !");
        }
        return RepeatStatus.FINISHED;
    }

    public void setInputFile(Resource inputFile) {
        this.inputFile = inputFile;
    }

    public void setTargetDirectory(String targetDirectory) {
        this.targetDirectory = targetDirectory;
    }

    public void setTargetFile(String targetFile) {
        this.targetFile = targetFile;
    }
}