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
    if (tokenizedQuery[4] == "NESTED"){
        parsedQuery.joinAlgorithm = NESTED;
    }
    else if (tokenizedQuery[4] == "PARTHASH"){
        parsedQuery.joinAlgorithm = PARTHASH;
    }
    parsedQuery.joinFirstRelationName = tokenizedQuery[5];
    parsedQuery.joinSecondRelationName = tokenizedQuery[6];
    parsedQuery.joinFirstColumnName = tokenizedQuery[8];
    parsedQuery.joinSecondColumnName = tokenizedQuery[10];
    parsedQuery.joinBufferSize = stoi(tokenizedQuery[12]);

    string binaryOperator = tokenizedQuery[9];
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

    // execute join algorithm here using nested loop join
    if (parsedQuery.joinAlgorithm == NESTED){
        vector<Page> blocksA(parsedQuery.joinBufferSize-2);
        vector<vector<int>> rowsInResultPage;
        int blockRowCounter = 0;

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
        if(blockRowCounter > 0){
            bufferManager.writePage(resultTable->tableName, resultTable->blockCount, rowsInResultPage, blockRowCounter);
            resultTable->blockCount++;
            resultTable->rowsPerBlockCount.emplace_back(blockRowCounter);
            blockRowCounter = 0;
            rowsInResultPage.clear();
        }

    }
    else if (parsedQuery.joinAlgorithm == PARTHASH){

    }
    else{
        cout << "SEMANTIC ERROR: No such algorithm exists" << endl;
        return;
    }

    tableCatalogue.insertTable(resultTable);
    cout << "Block Accesses: " << BLOCK_ACCESSES << endl;
    cout << "Generated Table "<< resultTable->tableName << ". Column Count: " << resultTable->columnCount << " Row Count: " << resultTable->rowCount << endl;
    return;
}