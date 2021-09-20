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
                vector<vector<int>> subMatrixA;
                subMatrixA = pageA->getRows();

                if(transpose(subMatrixA)){
                    pageA->rows = subMatrixA;
                    pageA->rowCount = subMatrixA.size();
                    pageA->columnCount = subMatrixA[0].size();
                    pageA->writePage();
                }
                else{
                    pageA->clearPage();
                }
            }
            else{
                Page * pageA = bufferManager.getPage(matrix->matrixName, rowBlockIndex, colBlockIndex);
                Page * pageB = bufferManager.getPage(matrix->matrixName, colBlockIndex, rowBlockIndex);
                vector<vector<int>> subMatrixA, subMatrixB;
                subMatrixA = pageA->getRows();
                subMatrixB = pageB->getRows();

                if(transpose(subMatrixB)){
                    pageA->rows = subMatrixB;
                    pageA->rowCount = subMatrixB.size();
                    pageA->columnCount = subMatrixB[0].size();
                    pageA->writePage();
                }
                else{
                    pageA->clearPage();
                }

                if(transpose(subMatrixA)){
                    pageB->rows = subMatrixA;
                    pageB->rowCount = subMatrixA.size();
                    pageB->columnCount = subMatrixA[0].size();
                    pageB->writePage();
                }
                else{
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