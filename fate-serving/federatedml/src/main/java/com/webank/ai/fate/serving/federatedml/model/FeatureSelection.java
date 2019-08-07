package com.webank.ai.fate.serving.federatedml.model;
import com.webank.ai.fate.core.constant.StatusCode;

import com.webank.ai.fate.core.mlmodel.buffer.FeatureSelectionParamProto.LeftCols;
import com.webank.ai.fate.core.mlmodel.buffer.FeatureSelectionParamProto.FeatureSelectionParam;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import com.webank.ai.fate.serving.core.bean.Context;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class FeatureSelection extends BaseModel {
    private FeatureSelectionParam featureSelectionParam;
    private LeftCols finalLeftCols;
    private static final Logger LOGGER = LogManager.getLogger();

    @Override
    public int initModel(byte[] protoMeta, byte[] protoParam) {
        LOGGER.info("start init Feature Selection class");
        try {
            this.featureSelectionParam = FeatureSelectionParam.parseFrom(protoParam);
            this.finalLeftCols = featureSelectionParam.getFinalLeftCols();
        } catch (Exception ex) {
            ex.printStackTrace();
            return StatusCode.ILLEGALDATA;
        }
        LOGGER.info("Finish init Feature Selection class");
        return StatusCode.OK;
    }

    @Override
    public Map<String, Object> predict(Context context , List<Map<String, Object> > inputData, Map<String, Object> predictParams) {
        LOGGER.info("Start Feature Selection predict");
        HashMap<String, Object> outputData = new HashMap<>();
        Map<String, Object> firstData = inputData.get(0);
        for (String key: firstData.keySet()) {
            Boolean isLeft = this.finalLeftCols.getLeftColsMap().get(key);
            if (isLeft) {
                outputData.put(key, firstData.get(key));
            }
        }
        return outputData;
    }

}
