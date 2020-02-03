package com.equifax.talentfactory.pipelineoptions;

import org.apache.beam.sdk.options.*;

public interface WordCounterPipelineOptions extends PipelineOptions {
    /**
     * By default, this example reads from a public dataset containing the text of King Lear. Set
     * this option to choose a different input file or glob.
     */
    @Description("Path of the file to read from")
    @Default.String("gs://apache-beam-samples/shakespeare/kinglear.txt")
    String getInputFile();

    void setInputFile(String value);

    /** Set this required option to specify where to write the output. */
    @Description("Path of the file to write to")
    @Validation.Required
    String getOutput();

    void setOutput(String value);
}
