package com.huawei.datasight.molap.datastats;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import com.huawei.datasight.molap.autoagg.model.AggSuggestion;
import com.huawei.datasight.molap.datastats.model.Level;
import com.huawei.datasight.molap.datastats.util.AggCombinationGeneratorUtil;
import com.huawei.iweb.platform.logging.LogService;
import com.huawei.iweb.platform.logging.LogServiceFactory;
import com.huawei.unibi.molap.engine.querystats.Preference;
import com.huawei.unibi.molap.engine.util.MolapEngineLogEvent;
import com.huawei.unibi.molap.util.MolapProperties;

/**
 * This class generates aggregate combination based on distinct data calculated
 * for data stats aggregation
 * 
 * @author A00902717
 *
 */
public class AggCombinationGenerator
{

	private Level[] levelDetails;

	private BigInteger maxPossibleRows;
	
	private int benefitRatio=Preference.BENEFIT_RATIO;
	
	/**
	 * Attribute for Molap LOGGER
	 */
	private static final LogService LOGGER = LogServiceFactory
			.getLogService(AggCombinationGenerator.class.getName());

	public AggCombinationGenerator(Level[] dimensionDistinctData, String table)
	{
		this.levelDetails = dimensionDistinctData;

		this.maxPossibleRows = AggCombinationGeneratorUtil
				.getMaxPossibleRows(levelDetails);
		String confBenefitRatio=MolapProperties.getInstance().getProperty("molap.agg.benefit.ratio");
        
        if(null!=confBenefitRatio)
        {
            benefitRatio=Integer.parseInt(confBenefitRatio);
            
        }

	}

	/**
	 * Generate all possible aggregate combinations
	 * 
	 * @return
	 */

	public List<AggSuggestion> generateAggregate()
	{
		
		List<AggSuggestion> allCombinations = AggCombinationGeneratorUtil
				.generateCombination(Arrays.asList(levelDetails),
						maxPossibleRows, benefitRatio);

		for (AggSuggestion comb : allCombinations)
		{
			LOGGER.info(MolapEngineLogEvent.UNIBI_MOLAPENGINE_MSG,
					"Processing:" + comb);

		}
		return allCombinations;
	}

}
