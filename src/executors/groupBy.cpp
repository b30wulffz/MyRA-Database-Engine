#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseGROUPBY()
{
    logger.log("syntacticParseGROUPBY");
    if (tokenizedQuery.size() != 9 || tokenizedQuery[2] != "GROUP" || tokenizedQuery[3] != "BY") {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = GROUPBY;
    parsedQuery.groupByResultRelationName = tokenizedQuery[0];
    parsedQuery.groupByColumnName = tokenizedQuery[4];
    parsedQuery.groupByRelationName = tokenizedQuery[6];
    
    string aggregationType = tokenizedQuery[8].substr(0, 3);
    string aggregationCol = tokenizedQuery[8].substr(4);
    aggregationCol = aggregationCol.substr(0, aggregationCol.size() - 1);
    if(aggregationCol.length() == 0)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.groupByAggregateColumnName = aggregationCol;

    if (aggregationType == "MAX")
        parsedQuery.groupByAggregate = MAX;
    else if (aggregationType == "MIN")
        parsedQuery.groupByAggregate = MIN;
    else if (aggregationType == "SUM")
        parsedQuery.groupByAggregate = SUM;
    else if (aggregationType == "AVG")
        parsedQuery.groupByAggregate = AVG;
    else {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseGROUPBY()
{
    logger.log("semanticParseGROUPBY");

    if (tableCatalogue.isTable(parsedQuery.groupByResultRelationName)) {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.groupByColumnName, parsedQuery.groupByRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.groupByAggregateColumnName, parsedQuery.groupByRelationName)) {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if(parsedQuery.groupByAggregate == NO_AGG_CLAUSE){
        cout << "SEMANTIC ERROR: Aggregate operation not specified" << endl;
        return false;
    }

    return true;
}



void executeGROUPBY()
{
    logger.log("executeGROUPBY");

    Table* inputTable = tableCatalogue.getTable(parsedQuery.groupByRelationName);

    string resultColumnName;
    if(parsedQuery.groupByAggregate == MAX){
        resultColumnName = "MAX" + parsedQuery.groupByAggregateColumnName;
    }
    else if(parsedQuery.groupByAggregate == MIN){
        resultColumnName = "MIN" + parsedQuery.groupByAggregateColumnName;
    }
    else if(parsedQuery.groupByAggregate == SUM){
        resultColumnName = "SUM" + parsedQuery.groupByAggregateColumnName;
    }
    else if(parsedQuery.groupByAggregate == AVG){
        resultColumnName = "AVG" + parsedQuery.groupByAggregateColumnName;
    }

    vector<string> columns = {parsedQuery.groupByColumnName, resultColumnName};
    
    Table* resultTable = new Table(parsedQuery.groupByResultRelationName, columns);
    resultTable->initStatistics();
    vector<vector<int>> rowsInResultPage;
    int blockRowCounter = 0;

    int grouppedColumnIndex = inputTable->getColumnIndex(parsedQuery.groupByColumnName);
    int aggregateColumnIndex = inputTable->getColumnIndex(parsedQuery.groupByAggregateColumnName);
    for(auto key: inputTable->distinctValuesInColumns[grouppedColumnIndex]){
        int aggregateValue, rowCount = 0;
        if(parsedQuery.groupByAggregate == MAX){   
            aggregateValue = INT_MIN;
        }
        else if(parsedQuery.groupByAggregate == MIN){
            aggregateValue = INT_MAX;
        }
        else if(parsedQuery.groupByAggregate == SUM || parsedQuery.groupByAggregate == AVG){
            aggregateValue = 0;
        }

        for(int blockId=0; blockId<inputTable->blockCount; blockId++){
            Page* block = bufferManager.getPage(inputTable->tableName, blockId);
            vector<vector<int>> rows = block->getRows();
            for(int rowInd=0; rowInd<block->rowCount; rowInd++){
                if(rows[rowInd][grouppedColumnIndex] == key){
                    switch(parsedQuery.groupByAggregate){
                        case MAX:
                            if(rows[rowInd][aggregateColumnIndex] > aggregateValue)
                                aggregateValue = rows[rowInd][aggregateColumnIndex];
                            break;
                        case MIN:
                            if(rows[rowInd][aggregateColumnIndex] < aggregateValue)
                                aggregateValue = rows[rowInd][aggregateColumnIndex];
                            break;
                        case SUM:
                            aggregateValue += rows[rowInd][aggregateColumnIndex];
                            break;
                        case AVG:
                            aggregateValue += rows[rowInd][aggregateColumnIndex];
                            rowCount++;
                            break;
                    }
                }
            }
        }
        if(parsedQuery.groupByAggregate == AVG){
            aggregateValue /= rowCount;
        }
        vector<int> row = {key, aggregateValue};
        rowsInResultPage.push_back(row);
        blockRowCounter++;
        resultTable->updateStatistics(row);
        if(blockRowCounter == resultTable->maxRowsPerBlock){
            bufferManager.writePage(resultTable->tableName, resultTable->blockCount, rowsInResultPage, blockRowCounter);
            resultTable->blockCount++;
            resultTable->rowsPerBlockCount.emplace_back(blockRowCounter);
            blockRowCounter = 0;
            rowsInResultPage.clear();
        }
    }

    if(blockRowCounter > 0){
        bufferManager.writePage(resultTable->tableName, resultTable->blockCount, rowsInResultPage, blockRowCounter);
        resultTable->blockCount++;
        resultTable->rowsPerBlockCount.emplace_back(blockRowCounter);
        blockRowCounter = 0;
        rowsInResultPage.clear();
    }

    tableCatalogue.insertTable(resultTable);
    cout << "Generated Table "<< resultTable->tableName << ". Column Count: " << resultTable->columnCount << " Row Count: " << resultTable->rowCount << endl;
    return;
}