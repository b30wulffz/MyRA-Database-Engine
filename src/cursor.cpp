#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}


Cursor::Cursor(string matrixName, int pageRowIndex, int pageColIndex)
{
    logger.log("Cursor::Cursor::Matrix");
    this->page = bufferManager.getPage(matrixName, pageRowIndex, pageColIndex);
    this->pagePointer = 0;
    this->tableName = matrixName;
    this->pageRowIndex = pageRowIndex;
    this->pageColIndex = pageColIndex;
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
    logger.log("Cursor::getNext");
    if(pageIndex == -1){ // for matrix
        Matrix* matrix = matrixCatalogue.getMatrix(this->tableName);
        uint rowIndex = this->pagePointer / matrix->colBlocks; 
        uint colBlockIndex = this->pagePointer % matrix->colBlocks;

        uint rowBlockIndex = rowIndex / BLOCK_ROW_COUNT;
        uint rowInBlock = rowIndex % BLOCK_ROW_COUNT;

        this->nextPage(rowBlockIndex, colBlockIndex);
        vector<int> result = this->page->getRow(rowInBlock);

        this->pagePointer++;
        return result;
    }
    else{
        vector<int> result = this->page->getRow(this->pagePointer); // SP: pagepointer means current row in a page
        this->pagePointer++;
        if (result.empty()) { // SP: If row pointer points to a row index larger than the one stored in current page/block
            tableCatalogue.getTable(this->tableName)->getNextPage(this);
            if (!this->pagePointer) { // SP: When pagepointer > Row count in a page
                result = this->page->getRow(this->pagePointer);
                this->pagePointer++;
            }
        }
        return result;
    }
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

/**
 * @brief Function that loads Page indicated by pageRowIndex and paeColIndex. Now 
 * the cursor starts reading from the new page.
 *
 * @param pageRowIndex 
 * @param pageColIndex 
 */
void Cursor::nextPage(int pageRowIndex, int pageColIndex) // SP: Get the next page from temp, when rows exceed page's length
{
    logger.log("Cursor::nextPage::Matrix");
    this->page = bufferManager.getPage(this->tableName, pageRowIndex, pageColIndex);
    this->pageRowIndex = pageRowIndex;
    this->pageColIndex = pageColIndex;
}