package io.github.vdubois.tasklet;

import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;
import org.w3c.dom.Node;
import org.w3c.dom.bootstrap.DOMImplementationRegistry;
import org.w3c.dom.ls.DOMImplementationLS;
import org.w3c.dom.ls.LSOutput;
import org.w3c.dom.ls.LSSerializer;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.StringReader;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;

/**
 * Created by vdubois on 26/11/16.
 */
public class XMLPrettifyTasklet implements Tasklet {

    private Resource inputFile;

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

        String xmlToTransform = FileUtils.readFileToString(inputFile.getFile(), StandardCharsets.UTF_8.name());
        final InputSource inputSource = new InputSource(new StringReader(xmlToTransform));
        final Node document = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(inputSource).getDocumentElement();
        final boolean keepDeclaration = xmlToTransform.startsWith("<?xml");

        final DOMImplementationRegistry registry = DOMImplementationRegistry.newInstance();
        final DOMImplementationLS domImplementation = (DOMImplementationLS) registry.getDOMImplementation("LS");
        final LSSerializer writer = domImplementation.createLSSerializer();
        StringWriter stringWriter = new StringWriter();
        final LSOutput output = domImplementation.createLSOutput();
        output.setEncoding(StandardCharsets.UTF_8.name());
        output.setCharacterStream(stringWriter);

        writer.getDomConfig().setParameter("format-pretty-print", true);
        writer.getDomConfig().setParameter("xml-declaration", keepDeclaration);

        writer.write(document, output);
        String transformedXML = stringWriter.toString();

        FileUtils.writeStringToFile(inputFile.getFile(), transformedXML);
        return RepeatStatus.FINISHED;
    }

    /**
     * Sets input file.
     *
     * @param inputFile the input file
     */
    public void setInputFile(Resource inputFile) {
        this.inputFile = inputFile;
    }
}