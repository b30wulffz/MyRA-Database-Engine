#include "global.h"

BufferManager::BufferManager() // SP: Deals with pages
{
    logger.log("BufferManager::BufferManager");
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page* BufferManager::getPage(string tableName, int pageIndex)
{
    logger.log("BufferManager::getPage");
    string pageName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else
        return this->insertIntoPool(tableName, pageIndex); // SP: Loads a page from tmp folder, stoes its data in a 2d vector, and saves entire instance in pool
}

/**
 * @brief Function called to read a page from the buffer manager. If the page is
 * not present in the pool, the page is read and then inserted into the pool.
 *
 * @param matrixName 
 * @param pageRowIndex 
 * @param pageColIndex 
 * @return Page 
 */
Page* BufferManager::getPage(string matrixName, int pageRowIndex, int pageColIndex)
{
    logger.log("BufferManager::getPage::Matrix");
    string pageName = "../data/temp/" +  matrixName + "_Page_R" + to_string(pageRowIndex) + "_C" + to_string(pageColIndex);
    if (this->inPool(pageName))
        return this->getFromPool(pageName);
    else
        return this->insertIntoPool(matrixName, pageRowIndex, pageColIndex); // SP: Loads a page from tmp folder, stoes its data in a 2d vector, and saves entire instance in pool
}


/**
 * @brief Checks to see if a page exists in the pool
 *
 * @param pageName 
 * @return true 
 * @return false 
 */
bool BufferManager::inPool(string pageName)
{
    logger.log("BufferManager::inPool");
    for (auto &page : this->pages) {
        if (pageName == page->pageName)
            return true;
    }
    return false;
}

/**
 * @brief If the page is present in the pool, then this function returns the
 * page. Note that this function will fail if the page is not present in the
 * pool.
 *
 * @param pageName 
 * @return Page 
 */
Page* BufferManager::getFromPool(string pageName)
{
    logger.log("BufferManager::getFromPool");
    for (auto &page : this->pages)
        if (pageName == page->pageName)
            return page;
}

/**
 * @brief Inserts page indicated by tableName and pageIndex into pool. If the
 * pool is full, the pool ejects the oldest inserted page from the pool and adds
 * the current page at the end. It naturally follows a queue data structure. 
 *
 * @param tableName 
 * @param pageIndex 
 * @return Page 
 */
Page* BufferManager::insertIntoPool(string tableName, int pageIndex)
{
    logger.log("BufferManager::insertIntoPool");
    if (this->pages.size() >= BLOCK_COUNT){ // SP: maintained a DEQUE for pages, with BLOCK_COUNT to be the allowed size of pages that can be brought to main memory
        Page* front = pages.front();
        pages.pop_front();
        // free up memory
        delete front;
    }
    Page* page = new Page(tableName, pageIndex);
    pages.push_back(page);
    return page;
}

/**
 * @brief Inserts page indicated by matrixName, pageRowIndex and pageColIndex 
 * into pool. If the pool is full, the pool ejects the oldest inserted page 
 * from the pool and adds the current page at the end. It naturally follows a 
 * queue data structure. 
 *
 * @param matrixName 
 * @param pageRowIndex 
 * @param pageColIndex 
 * @return Page 
 */
Page* BufferManager::insertIntoPool(string matrixName, int pageRowIndex, int pageColIndex)
{
    logger.log("BufferManager::insertIntoPool::Matrix");
    if (this->pages.size() >= BLOCK_COUNT){
        Page* front = pages.front();
        pages.pop_front();
        // free up memory
        delete front;
    }
    Page* page = new Page(matrixName, pageRowIndex, pageColIndex);
    pages.push_back(page);
    return page;
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName 
 * @param pageIndex 
 * @param rows 
 * @param rowCount 
 */
void BufferManager::writePage(string tableName, int pageIndex, vector<vector<int>> rows, int rowCount) // SP: rowCount basically tells how many rows are to be written from 2d matrix in the page having pageIndex
{
    logger.log("BufferManager::writePage");
    Page page(tableName, pageIndex, rows, rowCount);
    page.writePage();
}

/**
 * @brief The buffer manager is also responsible for writing pages. This is
 * called when new tables are created using assignment statements.
 *
 * @param tableName 
 * @param pageRowIndex 
 * @param pageColIndex 
 * @param rows 
 * @param rowCount 
 */
void BufferManager::writePage(string matrixName, int pageRowIndex, int pageColIndex, vector<vector<int>> rows, int rowCount) // SP: rowCount basically tells how many rows are to be written from 2d matrix in the page having pageIndex
{
    logger.log("BufferManager::writePage::Matrix");
    Page page(matrixName, pageRowIndex, pageColIndex, rows, rowCount);
    page.writePage();
}

/**
 * @brief This method is used to append data to a page
 *
 * @param tableName 
 * @param pageRowIndex 
 * @param pageColIndex 
 * @param row
 */
void BufferManager::appendToPage(string matrixName, int pageRowIndex, int pageColIndex, vector<int> row){
    Page* page = this->getPage(matrixName, pageRowIndex, pageColIndex);
    page->appendToPage(row);
}


/**
 * @brief Deletes file names fileName
 *
 * @param fileName 
 */
void BufferManager::deleteFile(string fileName)
{

    if (remove(fileName.c_str()))
        logger.log("BufferManager::deleteFile: Err");
    else
        logger.log("BufferManager::deleteFile: Success");
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the tableName and pageIndex.
 *
 * @param tableName 
 * @param pageIndex 
 */
void BufferManager::deleteFile(string tableName, int pageIndex)
{
    logger.log("BufferManager::deleteFile");
    string fileName = "../data/temp/" + tableName + "_Page" + to_string(pageIndex);
    this->deleteFile(fileName);
}

/**
 * @brief Overloaded function that calls deleteFile(fileName) by constructing
 * the fileName from the matrixName, pageRowIndex and pageColIndex.
 *
 * @param matrixName 
 * @param pageRowIndex 
 * @param pageColIndex 
 */
void BufferManager::deleteFile(string matrixName, int pageRowIndex, int pageColIndex)
{
    logger.log("BufferManager::deleteFile::Matrix");
    string fileName = "../data/temp/" + matrixName + "_Page_R" + to_string(pageRowIndex) + "_C" + to_string(pageColIndex);
    this->deleteFile(fileName);
}
