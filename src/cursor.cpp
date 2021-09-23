#include "global.h"

Cursor::Cursor(string tableName, int pageIndex)
{
    logger.log("Cursor::Cursor");
    this->page = bufferManager.getPage(tableName, pageIndex);
    this->pagePointer = 0;
    this->tableName = tableName;
    this->pageIndex = pageIndex;
}

Cursor::Cursor(string matrixName, int pageIndex, bool isSparse)
{
    logger.log("Cursor::Cursor::SparseMatrix");
    this->page = bufferManager.getPage(matrixName, pageIndex, isSparse);
    this->pagePointer = 0;
    this->tableName = matrixName;
    this->pageIndex = pageIndex;
}

Cursor::Cursor(string matrixName, int pageRowIndex, int pageColIndex, bool isMatrix)
{
    logger.log("Cursor::Cursor::Matrix");
    this->page = bufferManager.getPage(matrixName, pageRowIndex, pageColIndex, isMatrix);
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
    vector<int> result;
    switch(this->page->type){
        case SPARSE_MATRIX:
        {
            result = this->page->getRow(this->pagePointer); 
            this->pagePointer++;
            if (result.empty()) { 
                matrixCatalogue.getMatrix(this->tableName)->getNextPage(this);
                if (!this->pagePointer) { 
                    result = this->page->getRow(this->pagePointer);
                    this->pagePointer++;
                }
            }
        }
        break;
        case MATRIX:
        {
            Matrix* matrix = matrixCatalogue.getMatrix(this->tableName);
            uint rowIndex = this->pagePointer / matrix->colBlocks; 
            uint colBlockIndex = this->pagePointer % matrix->colBlocks;

            uint rowBlockIndex = rowIndex / BLOCK_ROW_COUNT;
            uint rowInBlock = rowIndex % BLOCK_ROW_COUNT;

            this->nextPage(rowBlockIndex, colBlockIndex, true);
            result = this->page->getRow(rowInBlock);

            this->pagePointer++;
        }
        break;
        case TABLE:
        case OTHER:
        {
            result = this->page->getRow(this->pagePointer); // SP: pagepointer means current row in a page
            this->pagePointer++;
            if (result.empty()) { // SP: If row pointer points to a row index larger than the one stored in current page/block
                tableCatalogue.getTable(this->tableName)->getNextPage(this);
                if (!this->pagePointer) { // SP: When pagepointer > Row count in a page
                    result = this->page->getRow(this->pagePointer);
                    this->pagePointer++;
                }
            }
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

/**
 * @brief Function that loads Page indicated by pageIndex. Now the cursor starts
 * reading from the new page.
 *
 * @param pageIndex 
 * @param isSparse
 */
void Cursor::nextPage(int pageIndex, bool isSparse) // SP: Get the next page from temp, when rows exceed page's length
{
    logger.log("Cursor::nextPage::SparseMatrix");
    this->page = bufferManager.getPage(this->tableName, pageIndex, isSparse);
    this->pageIndex = pageIndex;
    this->pagePointer = 0;
}

/**
 * @brief Function that loads Page indicated by pageRowIndex and paeColIndex. Now 
 * the cursor starts reading from the new page.
 *
 * @param pageRowIndex 
 * @param pageColIndex 
 * @param isMatrix
 */
void Cursor::nextPage(int pageRowIndex, int pageColIndex, bool isMatrix) // SP: Get the next page from temp, when rows exceed page's length
{
    logger.log("Cursor::nextPage::Matrix");
    this->page = bufferManager.getPage(this->tableName, pageRowIndex, pageColIndex, isMatrix);
    this->pageRowIndex = pageRowIndex;
    this->pageColIndex = pageColIndex;
}

/**
 * @brief Function returns the page index which cursor is pointing to
 * 
 */
int Cursor::getPageIndex()
{
    logger.log("Cursor::getPageIndex");
}