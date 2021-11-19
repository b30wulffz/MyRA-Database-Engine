#include "global.h"
/**
 * @brief 
 * SYNTAX: <new_table> <- GROUP BY <grouping_attribute> FROM <table_name> RETURN MAX|MIN|SUM|AVG(<attribute>)
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

    BLOCK_ACCESSES = 0;

    Table* inputTable = tableCatalogue.getTable(parsedQuery.groupByRelationName);

    string resultColumnName;
    switch(parsedQuery.groupByAggregate){
        case MAX:
            resultColumnName = "MAX";
            break;
        case MIN:
            resultColumnName = "MIN";
            break;
        case SUM:
            resultColumnName = "SUM";
            break;
        case AVG:
            resultColumnName = "AVG";
            break;
    }
    resultColumnName += parsedQuery.groupByAggregateColumnName;
    vector<string> columns = {parsedQuery.groupByColumnName, resultColumnName};
    
    Table* resultTable = new Table(parsedQuery.groupByResultRelationName, columns);
    resultTable->initStatistics();
    vector<vector<int>> rowsInResultPage;
    int blockRowCounter = 0;

    int grouppedColumnIndex = inputTable->getColumnIndex(parsedQuery.groupByColumnName);
    int aggregateColumnIndex = inputTable->getColumnIndex(parsedQuery.groupByAggregateColumnName);

    unordered_map<int, int> groupByMap, rowCountMap;
    for(auto key: inputTable->distinctValuesInColumns[grouppedColumnIndex]){
        switch(parsedQuery.groupByAggregate){
            case MAX:
                groupByMap[key] = INT_MIN;
                break;
            case MIN:
                groupByMap[key] = INT_MAX;
                break;
            case SUM:
            case AVG:
                groupByMap[key] = 0;
                break;
        }
        rowCountMap[key] = 0;
    }

    for(int blockId=0; blockId<inputTable->blockCount; blockId++){
        Page* block = bufferManager.getPage(inputTable->tableName, blockId);
        vector<vector<int>> rows = block->getRows();
        for(int rowInd=0; rowInd<block->rowCount; rowInd++){
            int grouppedColumnKey = rows[rowInd][grouppedColumnIndex];
            switch(parsedQuery.groupByAggregate){
                case MAX:
                    if(rows[rowInd][aggregateColumnIndex] > groupByMap[grouppedColumnKey])
                        groupByMap[grouppedColumnKey] = rows[rowInd][aggregateColumnIndex];
                    break;
                case MIN:
                    if(rows[rowInd][aggregateColumnIndex] < groupByMap[grouppedColumnKey])
                        groupByMap[grouppedColumnKey] = rows[rowInd][aggregateColumnIndex];
                    break;
                case SUM:
                case AVG:
                    groupByMap[grouppedColumnKey] += rows[rowInd][aggregateColumnIndex];
                    break;
            }
            rowCountMap[grouppedColumnKey]++;
        }
    }

    for(auto key: inputTable->distinctValuesInColumns[grouppedColumnIndex]){
        vector<int> row;
        row.push_back(key);
        switch(parsedQuery.groupByAggregate){
            case MAX:
            case MIN:
            case SUM:
                row.push_back(groupByMap[key]);
                break;
            case AVG:
                row.push_back(groupByMap[key]/rowCountMap[key]);
                break;
        }
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
    cout << "Block Accesses: " << BLOCK_ACCESSES << endl;
    cout << "Generated Table "<< resultTable->tableName << ". Column Count: " << resultTable->columnCount << " Row Count: " << resultTable->rowCount << endl;
    return;
}