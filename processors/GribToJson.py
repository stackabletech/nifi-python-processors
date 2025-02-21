from nifiapi.flowfiletransform import FlowFileTransform, FlowFileTransformResult

class GribToJson(FlowFileTransform):
    class Java:
        implements = ['org.apache.nifi.python.processor.FlowFileTransform']
    class ProcessorDetails:
        version = '0.0.1-SNAPSHOT'
        dependencies = ['xarray', 'cfgrib']

    def __init__(self, **kwargs):
        pass

    def transform(self, context, flowfile):
        # Import Python dependencies
        import xarray as xr 
        try:
            # Don't even try to parse an empty flowfile
            if flowfile.getSize() == 0:
                self.logger.error("Failed to parse GRIB data: empty file")
                return FlowFileTransformResult(relationship = "failure")

            input = xr.open_dataset(flowfile.getContentsAsBytes(), engine='cfgrib', filter_by_keys={'typeOfLevel': 'meanSea'})
            output = input.to_dataframe().to_json()
            
            # Extract the attributes from the NetCDF files as flowfile attributes
            attrs = input.attrs
            for key in attrs.keys():
                attrs[key] = str(attrs[key])

            # Set the MIME type attribute to CSV
            attrs['mime.type'] = "application/json"

            return FlowFileTransformResult(
            relationship = "success",
            contents = output,
            attributes = attrs
            )

        except Exception as e:
            self.logger.error(e)
            return FlowFileTransformResult(relationship = "failure")
        
