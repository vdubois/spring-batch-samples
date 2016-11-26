package io.github.vdubois.callback;

import org.springframework.batch.item.file.FlatFileFooterCallback;
import org.springframework.batch.item.file.FlatFileHeaderCallback;

import java.io.IOException;
import java.io.Writer;

/**
 * Created by vdubois on 26/11/16.
 */
public class FormatterCallback implements FlatFileHeaderCallback, FlatFileFooterCallback {

    private String format;

    private String[] parameters;

    private boolean generateBom;

    @Override
    public void writeHeader(Writer writer) throws IOException {
        if (this.generateBom) {
            writer.write("\ufeff");
        }
        writer.write(String.format(format, parameters));
    }

    @Override
    public void writeFooter(Writer writer) throws IOException {
        writer.write(String.format(format, parameters));
    }

    public void setFormat(String format) {
        this.format = format;
    }

    public void setParameters(String[] parameters) {
        this.parameters = parameters;
    }

    public void setGenerateBom(boolean generateBom) {
        this.generateBom = generateBom;
    }
}
