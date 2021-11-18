#include "global.h"
/**
 * @brief 
 * SYNTAX: R <- JOIN relation_name1, relation_name2 ON column_name1 bin_op column_name2
 */
bool syntacticParseJOIN()
{
    logger.log("syntacticParseJOIN");
    if (tokenizedQuery.size() != 13 || tokenizedQuery[3] != "USING" || tokenizedQuery[7] != "ON") {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = JOIN;
    parsedQuery.joinResultRelationName = tokenizedQuery[0];
    
    parsedQuery.joinFirstRelationName = tokenizedQuery[5];
    parsedQuery.joinSecondRelationName = tokenizedQuery[6];
    parsedQuery.joinFirstColumnName = tokenizedQuery[8];
    parsedQuery.joinSecondColumnName = tokenizedQuery[10];
    parsedQuery.joinBufferSize = stoi(tokenizedQuery[12]);

    string binaryOperator = tokenizedQuery[9];

    if (tokenizedQuery[4] == "NESTED"){
        parsedQuery.joinAlgorithm = NESTED;
    }
    else if (tokenizedQuery[4] == "PARTHASH"){
        parsedQuery.joinAlgorithm = PARTHASH;
        if(binaryOperator != "=="){
            cout << "SYNTAX ERROR" << endl;
            return false;
        }
    }

    if (binaryOperator == "<")
        parsedQuery.joinBinaryOperator = LESS_THAN;
    else if (binaryOperator == ">")
        parsedQuery.joinBinaryOperator = GREATER_THAN;
    else if (binaryOperator == ">=" || binaryOperator == "=>")
        parsedQuery.joinBinaryOperator = GEQ;
    else if (binaryOperator == "<=" || binaryOperator == "=<")
        parsedQuery.joinBinaryOperator = LEQ;
    else if (binaryOperator == "==")
        parsedQuery.joinBinaryOperator = EQUAL;
    else if (binaryOperator == "!=")
        parsedQuery.joinBinaryOperator = NOT_EQUAL;
    else {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    return true;
}

bool semanticParseJOIN()
{
    logger.log("semanticParseJOIN");

    if (tableCatalogue.isTable(parsedQuery.joinResultRelationName)) {
        cout << "SEMANTIC ERROR: Resultant relation already exists" << endl;
        return false;
    }

    if (!tableCatalogue.isTable(parsedQuery.joinFirstRelationName) || !tableCatalogue.isTable(parsedQuery.joinSecondRelationName)) {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }

    if (!tableCatalogue.isColumnFromTable(parsedQuery.joinFirstColumnName, parsedQuery.joinFirstRelationName) || !tableCatalogue.isColumnFromTable(parsedQuery.joinSecondColumnName, parsedQuery.joinSecondRelationName)) {
        cout << "SEMANTIC ERROR: Column doesn't exist in relation" << endl;
        return false;
    }

    if(parsedQuery.joinAlgorithm == NO_JOIN_CLAUSE){
        cout << "SEMANTIC ERROR: Join algorithm not specified" << endl;
        return false;
    }

    if(parsedQuery.joinBufferSize < 3){
        cout << "SEMANTIC ERROR: Join buffer size must be greater than 2" << endl;
        return false;
    }

    return true;
}

bool customCompare(int a, int b, BinaryOperator op){
    if(op == LESS_THAN){
        return a < b;
    }
    else if(op == GREATER_THAN){
        return a > b;
    }
    else if(op == GEQ){
        return a >= b;
    }
    else if(op == LEQ){
        return a <= b;
    }
    else if(op == EQUAL){
        return a == b;
    }
    else if(op == NOT_EQUAL){
        return a != b;
    }
    else{
        return false;
    }
}

void partitionTable(Table* table, int tableColumnIndex, vector<int> &partitionBlockCount, vector<int> &blockRowCounter, vector<vector<int>> &rowsPerBlock, set<int> &validPartitionIds){
    logger.log("partitionTable");

    int partitionSize = parsedQuery.joinBufferSize - 1;
    vector<vector<vector<int>>> partitionedTable(partitionSize+1);
    partitionBlockCount.assign(partitionSize+1, 0); // number of blocks in each partition
    blockRowCounter.assign(partitionSize+1, 0); // total number of rows in a block
    rowsPerBlock.assign(partitionSize+1, vector<int>()); // number of rows in each block, for each partition

    int maxRowsPerBlock = table->maxRowsPerBlock;

    // tableName_attribute_partitionId_blockId
    for(int blockId=0; blockId<table->blockCount; blockId++){
        Page cachedBlock = *bufferManager.getPage(table->tableName, blockId);
        vector<vector<int>> rows = cachedBlock.getRows();
        for(int i=0; i<cachedBlock.rowCount; i++){
            int partitionId = rows[i][tableColumnIndex] % partitionSize;
            validPartitionIds.insert(partitionId);
            partitionedTable[partitionId].push_back(rows[i]);
            blockRowCounter[partitionId]++;
            if(blockRowCounter[partitionId] == maxRowsPerBlock){
                string partitionBlockName = table->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionId);
                bufferManager.writePage(partitionBlockName, partitionBlockCount[partitionId], partitionedTable[partitionId], blockRowCounter[partitionId]);
                partitionBlockCount[partitionId]++;
                rowsPerBlock[partitionId].push_back(blockRowCounter[partitionId]);
                blockRowCounter[partitionId] = 0;
                partitionedTable[partitionId].clear();
            }
        }
    }

    // check if any partition has at least one block
    for(auto partitionId : validPartitionIds){
        if(blockRowCounter[partitionId] > 0){
            string partitionBlockName = table->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionId);
            bufferManager.writePage(partitionBlockName, partitionBlockCount[partitionId], partitionedTable[partitionId], blockRowCounter[partitionId]);
            partitionBlockCount[partitionId]++;
            rowsPerBlock[partitionId].push_back(blockRowCounter[partitionId]);
            blockRowCounter[partitionId] = 0;
            partitionedTable[partitionId].clear();
        }
    }
    // unload table partitions from memory
    partitionedTable.clear();
}

void loopJoin(string tableNameA, int tableBlockCountA, int tableColumnIndexA, int tableColumnSizeA, int maxRowsPerBlockA, vector<int> &rowsPerBlockA, string tableNameB, int tableBlockCountB, int tableColumnIndexB, int tableColumnSizeB, int maxRowsPerBlockB, vector<int> &rowsPerBlockB, int bufferSize, Table* resultTable, bool &flipped, vector<vector<int>> &rowsInResultPage, int &blockRowCounter)
{
    logger.log("loopJoin");

    vector<Page> blocksA(bufferSize);

    // for A (outer table)
    for(int outerBlockId=0; outerBlockId<tableBlockCountA; outerBlockId+=bufferSize){
        
        int cachedBlockCount = min(int(tableBlockCountA-outerBlockId), bufferSize);
        blocksA.resize(cachedBlockCount);

        for(int i=0; i<cachedBlockCount; i++){
            if(rowsPerBlockA.size() > 0){
                TMP_ROW_COUNT = rowsPerBlockA[outerBlockId+i];
                TMP_COL_COUNT = tableColumnSizeA;
                TMP_MAX_ROWS_PER_BLOCK = maxRowsPerBlockA;
            }
            blocksA[i] = *bufferManager.getPage(tableNameA, outerBlockId+i);
        }

        // for B (inner table)
        for(int innerBlockId=0; innerBlockId<tableBlockCountB; innerBlockId++){
            if(rowsPerBlockB.size() > 0){
                TMP_ROW_COUNT = rowsPerBlockB[innerBlockId];
                TMP_COL_COUNT = tableColumnSizeB;
                TMP_MAX_ROWS_PER_BLOCK = maxRowsPerBlockB;
            }
            Page cachedInnerBlock = *bufferManager.getPage(tableNameB, innerBlockId);
            vector<vector<int>> rowsB = cachedInnerBlock.getRows();
            for(int i=0; i<cachedBlockCount; i++){
                Page cachedOuterBlock = blocksA[i];
                vector<vector<int>> rowsA = cachedOuterBlock.getRows();
                for(int j=0; j<cachedOuterBlock.rowCount; j++){
                    for(int k=0; k<cachedInnerBlock.rowCount; k++){
                        if(customCompare(rowsA[j][tableColumnIndexA], rowsB[k][tableColumnIndexB], parsedQuery.joinBinaryOperator)){
                            vector<int> row;
                            row.clear();
                            if(!flipped){
                                for(int l=0; l<tableColumnSizeA; l++){
                                    row.push_back(rowsA[j][l]);
                                }
                                for(int l=0; l<tableColumnSizeB; l++){
                                    row.push_back(rowsB[k][l]);
                                }
                            }
                            else {
                                for(int l=0; l<tableColumnSizeB; l++){
                                    row.push_back(rowsB[k][l]);
                                }
                                for(int l=0; l<tableColumnSizeA; l++){
                                    row.push_back(rowsA[j][l]);
                                }
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
                    }
                }
            }
        }
        blocksA.clear();
    }
    TMP_ROW_COUNT = 0;
    TMP_COL_COUNT = 0;
    TMP_MAX_ROWS_PER_BLOCK = 0;
}

void executeJOIN()
{
    logger.log("executeJOIN");

    BLOCK_ACCESSES = 0;

    Table* tableA = tableCatalogue.getTable(parsedQuery.joinFirstRelationName);
    Table* tableB = tableCatalogue.getTable(parsedQuery.joinSecondRelationName);
    
    bool flipped = false;
    if(tableB->blockCount > tableA->blockCount){
        swap(tableA, tableB);
        swap(parsedQuery.joinFirstColumnName, parsedQuery.joinSecondColumnName);
        flipped = true;
    }

    int tableAColumnIndex = tableA->getColumnIndex(parsedQuery.joinFirstColumnName);
    int tableBColumnIndex = tableB->getColumnIndex(parsedQuery.joinSecondColumnName);

    vector<string> columns;
    for(int i=0; i<tableA->columns.size(); i++){
        columns.push_back(tableA->columns[i]+"_"+tableA->tableName);
    }
    for(int i=0; i<tableB->columns.size(); i++){
        columns.push_back(tableB->columns[i]+"_"+tableB->tableName);
    }

    Table* resultTable = new Table(parsedQuery.joinResultRelationName, columns);
    resultTable->initStatistics();

    vector<vector<int>> rowsInResultPage;
    int blockRowCounter = 0; // rows within a (result) block

    // execute join algorithm here using nested loop join
    if (parsedQuery.joinAlgorithm == NESTED){
        vector<int> tmp;
        loopJoin(
            tableA->tableName,
            tableA->blockCount,
            tableAColumnIndex,
            tableA->columns.size(),
            tableA->maxRowsPerBlock,
            tmp,
            tableB->tableName,
            tableB->blockCount,
            tableBColumnIndex,
            tableB->columns.size(),
            tableB->maxRowsPerBlock,
            tmp,
            parsedQuery.joinBufferSize - 2,
            resultTable,
            flipped,
            rowsInResultPage,
            blockRowCounter
        );
    }
    else if (parsedQuery.joinAlgorithm == PARTHASH){
        // partition large table
        int partitionSize = parsedQuery.joinBufferSize - 1;
        vector<int> partitionBlockCountA; // number of blocks in each partition
        vector<int> blockRowCounterA; // total number of rows in a block
        vector<vector<int>> rowsPerBlockA; // number of rows in each block, for each partition
        set<int> validPartitionIdsA; // valid partition ids for table A

        partitionTable(tableA, tableAColumnIndex, partitionBlockCountA, blockRowCounterA, rowsPerBlockA, validPartitionIdsA);

        // partition small table
        vector<int> partitionBlockCountB; // number of blocks in each partition
        vector<int> blockRowCounterB; // total number of rows in a block
        vector<vector<int>> rowsPerBlockB; // number of rows in each block, for each partition
        set<int> validPartitionIdsB; // valid partition ids for table B

        partitionTable(tableB, tableBColumnIndex, partitionBlockCountB, blockRowCounterB, rowsPerBlockB, validPartitionIdsB);

        int i = 0;

        // join partitions
        for(auto partitionId : validPartitionIdsA){
            i++;
            if(validPartitionIdsB.find(partitionId) == validPartitionIdsB.end()){
                continue;
            }
            string partitionBlockNameA = tableA->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionId);
            string partitionBlockNameB = tableB->tableName + "_" + parsedQuery.joinSecondColumnName + "_" + to_string(partitionId);

            loopJoin(
                partitionBlockNameA, 
                partitionBlockCountA[partitionId], 
                tableAColumnIndex, 
                tableA->columns.size(),
                tableA->maxRowsPerBlock,
                rowsPerBlockA[partitionId],
                partitionBlockNameB, 
                partitionBlockCountB[partitionId], 
                tableBColumnIndex, 
                tableB->columns.size(),
                tableB->maxRowsPerBlock,
                rowsPerBlockB[partitionId],
                parsedQuery.joinBufferSize - 2,
                resultTable,
                flipped,
                rowsInResultPage,
                blockRowCounter
            );
        }
    }
    else{
        cout << "SEMANTIC ERROR: No such algorithm exists" << endl;
        return;
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