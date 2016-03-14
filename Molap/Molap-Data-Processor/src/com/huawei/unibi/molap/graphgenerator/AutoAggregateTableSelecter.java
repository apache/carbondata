package com.huawei.unibi.molap.graphgenerator;

import java.util.Arrays;
import java.util.List;

//import mondrian.olap.MondrianDef.Cube;

import com.huawei.unibi.molap.olap.MolapDef.Cube;
import com.huawei.unibi.molap.schema.metadata.AggregateTable;

public class AutoAggregateTableSelecter extends AggregateTableSelecter {
	private boolean isMatching;

	public AutoAggregateTableSelecter(List<AggregateTable> aggregateTables) {
		super(aggregateTables);

	}

	/**
	 * 
	 */
	public void selectTableForAggTableAggregationProcess(
			AggregateTable[] aggregateTablesArray, Cube cube) {

		AggregateTableDerivativeComposite aggregateTableDerivative = null;
		for (int i = 0; i < aggregateTables.size(); i++) {

			if (i == 0) {
				factName = cube.fact.getAlias();
				// aggregateTableDerivative = new
				// AggregateTableDerivativeComposite();
				root = new AggregateTableDerivativeComposite();
				aggregateTableDerivative = new AggregateTableDerivativeComposite(
						factName, aggregateTables.get(i));
				root.add(aggregateTableDerivative);
				// aggregateTableDerivative.add(aggregateTableDerivativeLocal);

			} else {

				//AggregateTableDerivative aggregateTableDerivativeLocal = aggregateTableDerivative;

				if (aggregateTableDerivative!=null) {

					addAggTableRelation(root,
							aggregateTables.get(i));
					if (!isMatching) {
						AggregateTableDerivative aggregateTableDerivativetemp = new AggregateTableDerivativeComposite(
								factName, aggregateTables.get(i));
						root.add(aggregateTableDerivativetemp);
					}
				}

			}

		}

	}

	private void addAggTableRelation(
			AggregateTableDerivative aggregateTableDerivative,
			AggregateTable aggregateTable) {

		List<AggregateTableDerivative> listOfAggregateTableDerivativeChildren = aggregateTableDerivative
				.getChildrens();

		AggregateTable aggregateTableOfDerivativecomp = null;

		if (listOfAggregateTableDerivativeChildren.isEmpty()) {
//			if (aggregateTableDerivative instanceof AggregateTableDerivativeComposite) {
				aggregateTableOfDerivativecomp = ((AggregateTableDerivativeComposite) aggregateTableDerivative)
						.getAggregateTable();
//			}
			isMatching = isMatchingTable(aggregateTable,
					aggregateTableOfDerivativecomp, isMatching);
			if (isMatching) {
				// addLeafNode(aggregateTable,aggregateTableOfDerivativecomp,aggregateTableDerivative);
				AggregateTableDerivative derivedTable = new AggregateTableDerivativeComposite(
						aggregateTableOfDerivativecomp.getAggregateTableName(),
						aggregateTable);
				if(!processedAggTables.contains(aggregateTable))
				{
					aggregateTableDerivative.add(derivedTable);
					processedAggTables.add(aggregateTable);
				}

			}
		} else {
			for (int j = listOfAggregateTableDerivativeChildren.size() - 1; j >= 0; j--) {

				AggregateTableDerivative aggTableDerivativeChild = listOfAggregateTableDerivativeChildren
						.get(j);

				addAggTableRelation(aggTableDerivativeChild, aggregateTable);

				if (!isMatching) {
					addNode(aggTableDerivativeChild, aggregateTable);
				}
			}
		}

	}

	private void addNode(AggregateTableDerivative aggTableDerivativeChild,
			AggregateTable aggregateTable) {
		AggregateTable aggregateTableOfDerivativecomp = null;

//		if (aggTableDerivativeChild instanceof AggregateTableDerivativeComposite) {
			aggregateTableOfDerivativecomp = ((AggregateTableDerivativeComposite) aggTableDerivativeChild)
					.getAggregateTable();
//		}
		isMatching = isMatchingTable(aggregateTable,
				aggregateTableOfDerivativecomp, isMatching);
		if (isMatching) {
			AggregateTableDerivative derivedTable = new AggregateTableDerivativeComposite(
					aggregateTableOfDerivativecomp.getAggregateTableName(),
					aggregateTable);
			if (!processedAggTables.contains(aggregateTable)) {
				aggTableDerivativeChild.add(derivedTable);
				processedAggTables.add(aggregateTable);
			}
		}

	}

	/*
	 * private void addCompositeNode(AggregateTable aggregateTable,
	 * AggregateTable aggregateTableOfDerivativecomp, AggregateTableDerivative
	 * aggTableDerivativeChild) { AggregateTableDerivative derivedTable = new
	 * AggregateTableDerivativeComposite( aggregateTableOfDerivativecomp
	 * .getAggregateTableName(), aggregateTable);
	 * aggTableDerivativeChild.add(derivedTable); }
	 */

	private boolean isMatchingTable(AggregateTable aggregateTable,
			AggregateTable aggregateTableOfDerivativecomp, boolean isMatching) {
		List<String> levelsList = Arrays.asList(aggregateTableOfDerivativecomp
				.getActualAggLevels());
		for (String level : aggregateTable.getActualAggLevels()) {
			isMatching = levelsList.contains(level);
			if (!isMatching) {
				return isMatching;
			}
		}
		return isMatching;
	}


}
