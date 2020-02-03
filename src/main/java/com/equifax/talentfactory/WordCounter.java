package com.equifax.talentfactory;

import com.equifax.talentfactory.beam.CountWords;
import com.equifax.talentfactory.beam.FormatAsTextFn;
import com.equifax.talentfactory.pipelineoptions.WordCounterPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;

public class WordCounter {
    public static void main(String[] args) {

        WordCounterPipelineOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(WordCounterPipelineOptions.class);

        Pipeline pipeline = Pipeline.create(options);

        PCollection<String> lines = pipeline.apply("ReadLines", TextIO.read().from(options.getInputFile()));

        PCollection<KV<String, Long>> wordCounter = lines.apply("Count Words", new CountWords());

        PCollection<String> wordCounterConcatenated = wordCounter.apply("Concatenate Words", MapElements.via(new FormatAsTextFn()));

        wordCounterConcatenated.apply("WriteCounts", TextIO.write().to(options.getOutput()));

        pipeline.run().waitUntilFinish();
    }
}
