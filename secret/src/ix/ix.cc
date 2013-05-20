
#include "ix.h"

IX_Manager* IX_Manager::_ix_manager = 0;

IX_Manager* IX_Manager::Instance()
{
    if(!_ix_manager)
        _ix_manager = new IX_Manager();

    return _ix_manager;
}

IX_Manager::IX_Manager()
{
}

IX_Manager::~IX_Manager()
{
}

