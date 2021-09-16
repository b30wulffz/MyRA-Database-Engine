#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINTMAT()
{
    logger.log("syntacticParsePRINTMAT");
    if (tokenizedQuery.size() != 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINTMAT;
    parsedQuery.printRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParsePRINTMAT()
{
    logger.log("semanticParsePRINTMAT");
    if (!matrixCatalogue.isMatrix(parsedQuery.printRelationName)) {
        cout << "SEMANTIC ERROR: Matrix doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINTMAT()
{
    logger.log("executePRINTMAT");
    Matrix* matrix = matrixCatalogue.getMatrix(parsedQuery.printRelationName); // SP: Parsed query stores current matrix name
    matrix->print();
    return;
}
