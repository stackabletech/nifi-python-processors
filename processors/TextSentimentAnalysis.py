from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class TextSentimentAnalysis(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        dependencies = ['transformers[tensorflow]', 'tf-keras', 'tensorflow']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowfile):
        from transformers import pipeline
        sentiment_pipeline = pipeline("sentiment-analysis")

        try:
            sentiment = sentiment_pipeline(str(flowfile.getContentsAsBytes()))
            attrs = {}
            attrs['sentiment.label'] = sentiment['label']
            attrs['sentiment.score'] = sentiment['score']

            return FlowFileTransformResult(
            relationship = "success",
            attributes = attrs
            )

        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")