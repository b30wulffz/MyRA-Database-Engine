#include "global.h"
/**
 * @brief 
 * SYNTAX: LOAD relation_name
 */
bool syntacticParseLOADMAT()
{
    logger.log("syntacticParseLOADMAT");
    if (tokenizedQuery.size() != 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = LOADMAT;
    parsedQuery.loadRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParseLOADMAT()
{
    logger.log("semanticParseLOADMAT");
    if (matrixCatalogue.isMatrix(parsedQuery.loadRelationName)) {
        cout << "SEMANTIC ERROR: Matrix already exists" << endl;
        return false;
    }

    if (!isFileExists(parsedQuery.loadRelationName)) {
        cout << "SEMANTIC ERROR: Data file doesn't exist" << endl;
        return false;
    }
    return true;
}

void executeLOADMAT()
{
    logger.log("executeLOADMAT");

    Matrix* matrix = new Matrix(parsedQuery.loadRelationName);
    if (matrix->load()) {
        matrixCatalogue.insertMatrix(matrix);
        cout << "Loaded Matrix. Column Count: " << matrix->columnCount << " Row Count: " << matrix->rowCount << endl;
    }
    return;
}