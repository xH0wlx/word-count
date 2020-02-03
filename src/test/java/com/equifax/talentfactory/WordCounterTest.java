
package com.equifax.talentfactory;

import com.equifax.talentfactory.beam.CountWords;
import com.equifax.talentfactory.beam.FormatAsTextFn;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.testing.ValidatesRunner;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class WordCounterTest {

    @Rule
    public TestPipeline testPipeline = TestPipeline.create();

    @Test
    @Ignore
    @Category(ValidatesRunner.class)
    public void givenFileWith20Lines_whenPipelineRun_theReturnCountedWordsAsFile(){

        PCollection<String> lines = testPipeline.apply("ReadLines", TextIO.read().from("src/test/resources/loverscomplaint.txt"));

        PCollection<KV<String, Long>> wordCounter = lines.apply("Count Words", new CountWords());

        PCollection<String> wordCounterConcatenated = wordCounter.apply("Concatenate Words", MapElements.via(new FormatAsTextFn()));

        wordCounterConcatenated.apply("WriteCounts", TextIO.write().to("src/test/resources/result/"));

        testPipeline.run().waitUntilFinish();
    }

    @Test
    @Category(ValidatesRunner.class)
    public void givenFileWith2Lines_whenPipelineRun_theReturnCountedWordsAsFile(){

        PCollection<String> lines = testPipeline.apply("ReadLines",
                Create.of("Hola bien ¿Como estas?", "Hola bien Hola").withCoder(StringUtf8Coder.of()));

        PCollection<KV<String, Long>> wordCounter = lines.apply("Count Words", new CountWords());

        PCollection<String> wordCounterConcatenated = wordCounter.apply("Concatenate Words", MapElements.via(new FormatAsTextFn()));

        PAssert.that(wordCounterConcatenated).containsInAnyOrder("Hola: 3", "estas: 1", "Como: 1", "bien: 2");

        testPipeline.run().waitUntilFinish();
    }
}