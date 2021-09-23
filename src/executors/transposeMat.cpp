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
    if(matrix->isSparse){
        for(int pageIndex = 0; pageIndex < matrix->blockCount; pageIndex++){
            Page * page = bufferManager.getPage(matrix->matrixName, pageIndex, true);
            vector<vector<int>> &rows = page->getRows();
            for(auto &r : rows){
                int row = r[0] / matrix->columnCount;
                int col = r[0] % matrix->columnCount;
                r[0] = (col*matrix->rowCount) + row;
            }
            sort(rows.begin(), rows.end());
            page->writePage();
        }
    }
    else{
        uint upperLimit = max(matrix->rowBlocks, matrix->colBlocks);
        for(int rowBlockIndex = 0; rowBlockIndex < upperLimit; rowBlockIndex++){
            for(int colBlockIndex = rowBlockIndex; colBlockIndex < upperLimit; colBlockIndex++){
                if(rowBlockIndex == colBlockIndex){
                    Page * pageA = bufferManager.getPage(matrix->matrixName, rowBlockIndex, colBlockIndex, true);
                    vector<vector<int>> &subMatrixA = pageA->getRows();
                    if(transpose(subMatrixA)){
                        pageA->rowCount = subMatrixA.size();
                        pageA->columnCount = subMatrixA[0].size();
                        pageA->writePage();
                    }
                    else{
                        pageA->clearPage();
                    }
                }
                else{
                    Page * pageA = bufferManager.getPage(matrix->matrixName, rowBlockIndex, colBlockIndex, true);
                    Page * pageB = bufferManager.getPage(matrix->matrixName, colBlockIndex, rowBlockIndex, true);
                
                    vector<vector<int>> &subMatrixA = pageA->getRows();
                    vector<vector<int>> &subMatrixB = pageB->getRows();

                    bool isA = transpose(subMatrixA);
                    bool isB = transpose(subMatrixB);

                    // memory efficient swap
                    vector<vector<int>> tempMatrix = move(subMatrixA);
                    subMatrixA = move(subMatrixB);
                    subMatrixB = move(tempMatrix);

                    pageA->rowCount = subMatrixA.size();
                    if(isB){
                        pageA->columnCount = subMatrixA[0].size();
                        pageA->writePage();
                    }
                    else{
                        pageA->columnCount = 0;
                        pageA->clearPage();
                    }

                    pageB->rowCount = subMatrixB.size();
                    if(isA){
                        pageB->columnCount = subMatrixB[0].size();
                        pageB->writePage();
                    }
                    else{
                        pageB->columnCount = 0;
                        pageB->clearPage();
                    }
                }
            }
        }

        uint tmp = matrix->rowBlocks;
        matrix->rowBlocks = matrix->colBlocks;
        matrix->colBlocks = tmp;
    }

    uint tmp = matrix->rowCount;
    matrix->rowCount = matrix->columnCount;
    matrix->columnCount = tmp;

    matrix->isTransposed = !matrix->isTransposed;
    return;
}