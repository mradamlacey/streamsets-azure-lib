package com.streamsets.stage.lib.sample;

import com.streamsets.pipeline.api.base.BaseEnumChooserValues;
import com.streamsets.pipeline.config.DataFormat;

public class DataFormatChooserValues extends BaseEnumChooserValues<DataFormat> {

    public DataFormatChooserValues() {
        super(
                DataFormat.TEXT,
                DataFormat.JSON,
                DataFormat.AVRO,
                DataFormat.DELIMITED,
                DataFormat.SDC_JSON,
                DataFormat.BINARY,
                DataFormat.PROTOBUF
        );
    }

}
