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
        # Import Python dependencies
        from transformers import pipeline

        sentiment_pipeline = pipeline("sentiment-analysis")

        try:
            input = str(flowfile.getContentsAsBytes())
            self.logger.info(f"Test to process: {input}")
            output = sentiment_pipeline(input)
            
            # Set the MIME type attribute to CSV
            attrs = dict()
            attrs['mime.type'] = "application/json"
            attrs['sentiment'] = str(output)

            return FlowFileTransformResult(
            relationship = "success",
            attributes = attrs
            )

        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")
        
