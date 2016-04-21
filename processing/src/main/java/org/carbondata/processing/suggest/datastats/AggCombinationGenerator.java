/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.carbondata.processing.suggest.datastats;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;

import org.carbondata.common.logging.LogService;
import org.carbondata.common.logging.LogServiceFactory;
import org.carbondata.core.util.CarbonProperties;
import org.carbondata.processing.suggest.autoagg.model.AggSuggestion;
import org.carbondata.processing.suggest.datastats.model.Level;
import org.carbondata.processing.suggest.datastats.util.AggCombinationGeneratorUtil;
import org.carbondata.query.querystats.Preference;
import org.carbondata.query.util.CarbonEngineLogEvent;

/**
 * This class generates aggregate combination based on distinct data calculated
 * for data stats aggregation
 *
 * @author A00902717
 */
public class AggCombinationGenerator {

  /**
   * Attribute for Carbon LOGGER
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(AggCombinationGenerator.class.getName());
  private Level[] levelDetails;
  private BigInteger maxPossibleRows;
  private int benefitRatio = Preference.BENEFIT_RATIO;

  public AggCombinationGenerator(Level[] dimensionDistinctData, String table) {
    this.levelDetails = dimensionDistinctData;

    this.maxPossibleRows = AggCombinationGeneratorUtil.getMaxPossibleRows(levelDetails);
    String confBenefitRatio =
        CarbonProperties.getInstance().getProperty("carbon.agg.benefit.ratio");

    if (null != confBenefitRatio) {
      benefitRatio = Integer.parseInt(confBenefitRatio);

    }

  }

  /**
   * Generate all possible aggregate combinations
   *
   * @return
   */

  public List<AggSuggestion> generateAggregate() {

    List<AggSuggestion> allCombinations = AggCombinationGeneratorUtil
        .generateCombination(Arrays.asList(levelDetails), maxPossibleRows, benefitRatio);

    for (AggSuggestion comb : allCombinations) {
      LOGGER.info(CarbonEngineLogEvent.UNIBI_CARBONENGINE_MSG, "Processing:" + comb);

    }
    return allCombinations;
  }

}
