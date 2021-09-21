#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseTRANSPOSEMAT()
{
    logger.log("syntacticParseTRANSPOSEMAT");
    if (tokenizedQuery.size() != 2) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = TRANSPOSEMAT;
    parsedQuery.exportRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParseTRANSPOSEMAT()
{
    logger.log("semanticParseTRANSPOSEMAT");
    // Table should exist
    if (matrixCatalogue.isMatrix(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such relation exists" << endl;
    return false;
}

bool transpose(vector<vector<int>> &matrix){
    uint rows = matrix.size();
    if(rows > 0){
        uint cols = matrix[0].size();

        uint upperLim = max(rows, cols);

        // resizing rows
        matrix.resize(upperLim, vector<int>(cols, -1));

        // resizing columns
        for(auto &row: matrix)
            row.resize(upperLim, -1);
        
        for(int i=0; i<upperLim; i++){
            for(int j=i+1; j<upperLim; j++){
                int temp = matrix[i][j];
                matrix[i][j] = matrix[j][i];
                matrix[j][i] = temp;
            }
        }

        // resizing rows
        matrix.resize(cols);

        // resizing columns
        for(auto &row: matrix)
            row.resize(rows);
        return true;
    }
    return false;
}

void executeTRANSPOSEMAT()
{
    logger.log("executeTRANSPOSEMAT");
    Matrix* matrix = matrixCatalogue.getMatrix(parsedQuery.exportRelationName);
    uint upperLimit = max(matrix->rowBlocks, matrix->colBlocks);
    for(int rowBlockIndex = 0; rowBlockIndex < upperLimit; rowBlockIndex++){
        for(int colBlockIndex = rowBlockIndex; colBlockIndex < upperLimit; colBlockIndex++){
            if(rowBlockIndex == colBlockIndex){
                Page * pageA = bufferManager.getPage(matrix->matrixName, rowBlockIndex, colBlockIndex);

                if(transpose(pageA->rows)){
                    pageA->rowCount = pageA->rows.size();
                    pageA->columnCount = pageA->rows[0].size();
                    pageA->writePage();
                }
                else{
                    pageA->clearPage();
                }
            }
            else{
                Page * pageA = bufferManager.getPage(matrix->matrixName, rowBlockIndex, colBlockIndex);
                Page * pageB = bufferManager.getPage(matrix->matrixName, colBlockIndex, rowBlockIndex);
               
                bool isA = transpose(pageA->rows);
                bool isB = transpose(pageB->rows);

                // memory efficient swap
                vector<vector<int>> tempMatrix = move(pageA->rows);
                pageA->rows = move(pageB->rows);
                pageB->rows = move(tempMatrix);

                pageA->rowCount = pageA->rows.size();
                if(isB){
                    pageA->columnCount = pageA->rows[0].size();
                    pageA->writePage();
                }
                else{
                    pageA->columnCount = 0;
                    pageA->clearPage();
                }

                pageB->rowCount = pageB->rows.size();
                if(isA){
                    pageB->columnCount = pageB->rows[0].size();
                    pageB->writePage();
                }
                else{
                    pageB->columnCount = 0;
                    pageB->clearPage();
                }
            }
        }
    }
    uint tmp = matrix->rowCount;
    matrix->rowCount = matrix->columnCount;
    matrix->columnCount = tmp;

    tmp = matrix->rowBlocks;
    matrix->rowBlocks = matrix->colBlocks;
    matrix->colBlocks = tmp;
    return;
}