#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

/**
 * @brief This function reads the next row from the page. The index of the
 * current row read from the page is indicated by the pagePointer(points to row
 * in page the cursor is pointing to).
 *
 * @return vector<int> 
 */
vector<int> Cursor::getNext() // SP: needs to be reworked as a row may or may not be present in a single page for a matrix
{
    logger.log("Cursor::geNext");
    vector<int> result = this->page.getRow(this->pagePointer); // SP: pagepointer means current row in a page
    this->pagePointer++;
    if (result.empty()) { // SP: If row pointer points to a row index larger than the one stored in current page/block
        tableCatalogue.getTable(this->tableName)->getNextPage(this);
        if (!this->pagePointer) { // SP: When pagepointer > Row count in a page
            result = this->page.getRow(this->pagePointer);
            this->pagePointer++;
        }
    }
    return result;
}
/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex 
 */
void Cursor::nextPage(int pageIndex) // SP: Get the next page from temp, when rows exceed page's length
{
    logger.log("Cursor::nextPage");
    this->page = bufferManager.getPage(this->tableName, pageIndex);
    this->pageIndex = pageIndex;
    this->pagePointer = 0;
}