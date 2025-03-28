from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult
from transformers import pipeline

class TextSentimentAnalysis(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        dependencies = ['transformers[tensorflow]', 'tf-keras', 'tensorflow']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowfile):
        # # Import Python dependencies
        # from transformers import pipeline

        model_id = "cardiffnlp/twitter-roberta-base-sentiment-latest"
        sentiment_pipeline = pipeline("sentiment-analysis", model=model_id)

        try:
            input = str(flowfile.getContentsAsBytes())
            self.logger.info(f"Text to process: {input}")
            output = sentiment_pipeline(input)
            
            attrs = {}
            attrs['sentiment.label'] = output[0]['label']
            attrs['sentiment.score'] = str(output[0]['score'])

            return FlowFileTransformResult(
            relationship = "success",
            attributes = attrs
            )

        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")