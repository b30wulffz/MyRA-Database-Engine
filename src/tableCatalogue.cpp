#include "global.h"

void TableCatalogue::insertTable(Table* table)
{
    logger.log("TableCatalogue::~insertTable"); 
    this->tables[table->tableName] = table;
}
void TableCatalogue::deleteTable(string tableName)
{
    logger.log("TableCatalogue::deleteTable"); 
    this->tables[tableName]->unload(); // SP: deleting temp files
    delete this->tables[tableName]; // SP: deleting address/memory
    this->tables.erase(tableName); // SP: Removing key
}
Table* TableCatalogue::getTable(string tableName)
{
    logger.log("TableCatalogue::getTable"); 
    Table *table = this->tables[tableName];
    return table;
}
bool TableCatalogue::isTable(string tableName)
{
    logger.log("TableCatalogue::isTable"); 
    if (this->tables.count(tableName))
        return true;
    return false;
}

bool TableCatalogue::isColumnFromTable(string columnName, string tableName)
{
    logger.log("TableCatalogue::isColumnFromTable"); 
    if (this->isTable(tableName))
    {
        Table* table = this->getTable(tableName);
        if (table->isColumn(columnName))
            return true;
    }
    return false;
}

void TableCatalogue::print()
{
    logger.log("TableCatalogue::print"); 
    cout << "\nRELATIONS" << endl;

    int rowCount = 0;
    for (auto rel : this->tables)
    {
        cout << rel.first << endl;
        rowCount++;
    }
    printRowCount(rowCount);
}

TableCatalogue::~TableCatalogue(){
    logger.log("TableCatalogue::~TableCatalogue"); 
    for(auto table: this->tables){
        table.second->unload();
        delete table.second;
    }
}
