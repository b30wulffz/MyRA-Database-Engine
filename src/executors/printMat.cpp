#include "global.h"
/**
 * @brief 
 * SYNTAX: PRINT relation_name
 */
bool syntacticParsePRINTMAT()
{
    logger.log("syntacticParsePRINTMAT");
    if (tokenizedQuery.size() != 2)
    {
        cout << "SYNTAX ERROR" << endl;
        return false;
    }
    parsedQuery.queryType = PRINT;
    parsedQuery.printRelationName = tokenizedQuery[1];
    return true;
}

bool semanticParsePRINTMAT()
{
    logger.log("semanticParsePRINTMAT");
    if (!tableCatalogue.isTable(parsedQuery.printRelationName))
    {
        cout << "SEMANTIC ERROR: Relation doesn't exist" << endl;
        return false;
    }
    return true;
}

void executePRINTMAT()
{
    logger.log("executePRINTMAT");
    Table* table = tableCatalogue.getTable(parsedQuery.printRelationName); // SP: Parsed query stores current table name
    table->print();
    return;
}
