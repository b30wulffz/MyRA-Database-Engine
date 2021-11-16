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
    int blockRowCounter = 0;

    // execute join algorithm here using nested loop join
    if (parsedQuery.joinAlgorithm == NESTED){
        vector<Page> blocksA(parsedQuery.joinBufferSize-2);

        // for A
        for(int blockId=0; blockId<tableA->blockCount; blockId+=(parsedQuery.joinBufferSize-2)){
            int cachedBlockCount = min(int(tableA->blockCount-blockId), parsedQuery.joinBufferSize-2);
            blocksA.resize(cachedBlockCount);
            // load n-2 blocks of table A
            for(int i=0; i<cachedBlockCount; i++){
                blocksA[i] = *bufferManager.getPage(tableA->tableName, blockId+i);
            }

            // for B
            for(int innerBlockId=0; innerBlockId<tableB->blockCount; innerBlockId++){
                Page cachedInnerBlock = *bufferManager.getPage(tableB->tableName, innerBlockId);
                vector<vector<int>> rowsB = cachedInnerBlock.getRows();
                for(int i=0; i<cachedBlockCount; i++){
                    Page cachedOuterBlock = blocksA[i];
                    vector<vector<int>> rowsA = cachedOuterBlock.getRows();
                    for(int j=0; j<cachedOuterBlock.rowCount; j++){
                        for(int k=0; k<cachedInnerBlock.rowCount; k++){
                            if(customCompare(rowsA[j][tableAColumnIndex], rowsB[k][tableBColumnIndex], parsedQuery.joinBinaryOperator)){
                                vector<int> row;
                                row.clear();
                                if(!flipped){
                                    for(int l=0; l<tableA->columns.size(); l++){
                                        row.push_back(rowsA[j][l]);
                                    }
                                    for(int l=0; l<tableB->columns.size(); l++){
                                        row.push_back(rowsB[k][l]);
                                    }
                                }
                                else {
                                    for(int l=0; l<tableB->columns.size(); l++){
                                        row.push_back(rowsB[k][l]);
                                    }
                                    for(int l=0; l<tableA->columns.size(); l++){
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
    }
    else if (parsedQuery.joinAlgorithm == PARTHASH){
        // partition large table
        int partitionSize = parsedQuery.joinBufferSize - 1;
        vector<vector<vector<int>>> partitionedTableA(partitionSize+1);
        vector<int> partitionBlockCountA(partitionSize+1, 0); // number of blocks in each partition
        vector<int> blockRowCounterA(partitionSize+1, 0); // number of rows in a block
        vector<vector<int>> rowsPerBlockA(partitionSize+1); // number of rows in each block, for each partition
        set<int> validPartitionIdsA; // valid partition ids for table A

        int maxRowsPerBlock = tableA->maxRowsPerBlock;

        // tableName_attribute_partitionId_blockId
        for(int blockId=0; blockId<tableA->blockCount; blockId++){
            // int partitionId = blockId % partitionSize;
            Page cachedBlock = *bufferManager.getPage(tableA->tableName, blockId);
            vector<vector<int>> rows = cachedBlock.getRows();
            for(int i=0; i<cachedBlock.rowCount; i++){
                int partitionId = rows[i][tableAColumnIndex] % partitionSize;
                validPartitionIdsA.insert(partitionId);
                partitionedTableA[partitionId].push_back(rows[i]);
                blockRowCounterA[partitionId]++;
                if(blockRowCounterA[partitionId] == maxRowsPerBlock){
                    string partitionBlockName = tableA->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionId);
                    bufferManager.writePage(partitionBlockName, partitionBlockCountA[partitionId], partitionedTableA[partitionId], blockRowCounterA[partitionId]);
                    partitionBlockCountA[partitionId]++;
                    rowsPerBlockA[partitionId].push_back(blockRowCounterA[partitionId]);
                    blockRowCounterA[partitionId] = 0;
                    partitionedTableA[partitionId].clear();
                }
            }
        }
        
        // check if any partition has at least one block
        for(auto partitionId : validPartitionIdsA){
            if(blockRowCounterA[partitionId] > 0){
                string partitionBlockName = tableA->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionId);
                bufferManager.writePage(partitionBlockName, partitionBlockCountA[partitionId], partitionedTableA[partitionId], blockRowCounterA[partitionId]);
                partitionBlockCountA[partitionId]++;
                rowsPerBlockA[partitionId].push_back(blockRowCounterA[partitionId]);
                blockRowCounterA[partitionId] = 0;
                partitionedTableA[partitionId].clear();
            }
        }
        // unload table A partitions from memory
        partitionedTableA.clear();

        // hash small table in main memory
        unordered_map<int, vector<vector<int>>> partitionedTableB;
        for(int blockId=0; blockId<tableB->blockCount; blockId++){
            Page cachedBlock = *bufferManager.getPage(tableB->tableName, blockId);
            vector<vector<int>> rows = cachedBlock.getRows();
            for(int i=0; i<cachedBlock.rowCount; i++){
                int partitionId = rows[i][tableBColumnIndex] % partitionSize;
                partitionedTableB[partitionId].push_back(rows[i]);
            }
        }

        // join partitions
        for(auto partitionIdA : validPartitionIdsA){
            // load partition blocks from disk
            for(int blockIdA = 0; blockIdA < partitionBlockCountA[partitionIdA]; blockIdA++){
                string partitionBlockName = tableA->tableName + "_" + parsedQuery.joinFirstColumnName + "_" + to_string(partitionIdA);
                TMP_ROW_COUNT = rowsPerBlockA[partitionIdA][blockIdA];
                TMP_COL_COUNT = tableA->columns.size();
                TMP_MAX_ROWS_PER_BLOCK = tableA->maxRowsPerBlock;
                Page cachedBlock = *bufferManager.getPage(partitionBlockName, blockIdA);
                vector<vector<int>> rowsA = cachedBlock.getRows();
                for(int rowIdA=0; rowIdA<rowsPerBlockA[partitionIdA][blockIdA]; rowIdA++){
                    int partitionIdB = rowsA[rowIdA][tableAColumnIndex] % partitionSize;
                    vector<vector<int>>& rowsB = partitionedTableB[partitionIdB];
                    
                    for(int rowIdB=0; rowIdB<rowsB.size(); rowIdB++){
                        if(rowsA[rowIdA][tableAColumnIndex] == rowsB[rowIdB][tableBColumnIndex]){
                            vector<int> row;
                            row.clear();
                            if(!flipped){
                                for(int l=0; l<tableA->columns.size(); l++){
                                    row.push_back(rowsA[rowIdA][l]);
                                }
                                for(int l=0; l<tableB->columns.size(); l++){
                                    row.push_back(rowsB[rowIdB][l]);
                                }
                            }
                            else {
                                for(int l=0; l<tableB->columns.size(); l++){
                                    row.push_back(rowsB[rowIdB][l]);
                                }
                                for(int l=0; l<tableA->columns.size(); l++){
                                    row.push_back(rowsA[rowIdA][l]);
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
        TMP_ROW_COUNT = 0;
        TMP_COL_COUNT = 0;
        TMP_MAX_ROWS_PER_BLOCK = 0;
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