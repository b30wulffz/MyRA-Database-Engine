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

void executeTRANSPOSEMAT()
{
    logger.log("executeTRANSPOSEMAT");
    Matrix* matrix = matrixCatalogue.getMatrix(parsedQuery.exportRelationName);
    // SP: logic to transpose
    return;
}