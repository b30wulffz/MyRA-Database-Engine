#include "global.h"

/**
 * @brief 
 * SYNTAX: EXPORT <relation_name> 
 */

bool syntacticParseEXPORTMAT()
{
    logger.log("syntacticParseEXPORTMAT");
    if (tokenizedQuery.size() != 3) {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = EXPORTMAT;
    parsedQuery.exportRelationName = tokenizedQuery[2];
    return true;
}

bool semanticParseEXPORTMAT()
{
    logger.log("semanticParseEXPORTMAT");
    if (matrixCatalogue.isMatrix(parsedQuery.exportRelationName))
        return true;
    cout << "SEMANTIC ERROR: No such matrix exists" << endl;
    return false;
}

void executeEXPORTMAT()
{
    logger.log("executeEXPORTMAT");
    Matrix* matrix = matrixCatalogue.getMatrix(parsedQuery.exportRelationName);
    matrix->makePermanent();
    return;
}