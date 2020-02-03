package com.equifax.talentfactory.beam;

import org.apache.beam.repackaged.core.org.apache.commons.lang3.StringUtils;
import org.apache.beam.sdk.transforms.DoFn;

public class ExtractWordsFn extends DoFn<String, String> {

    private static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

    @ProcessElement
    public void processElement(@Element String element, OutputReceiver<String> receiver) {
        String[] words = element.split(TOKENIZER_PATTERN, -1);
        for (String word : words) {
            if (!StringUtils.isBlank(word)) {
                receiver.output(word);
            }
        }
    }
}
