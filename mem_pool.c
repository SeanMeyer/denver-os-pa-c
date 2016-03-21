/*
 * Created by Ivo Georgiev on 2/9/16.
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate

    if (pool_store == NULL) {
        pool_store = calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        if (pool_store == NULL)
            return  ALLOC_FAIL;
        else
            return  ALLOC_OK;
    } else {
        return ALLOC_CALLED_AGAIN;
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables

    if (pool_store == NULL)
        return ALLOC_CALLED_AGAIN;

    free(pool_store);
    pool_store = NULL;
    pool_store_size = 0;
    pool_store_capacity = 0;
    return  ALLOC_OK;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    if (pool_store == NULL)
        return NULL;

    // expand the pool store, if necessary
    _mem_resize_pool_store();

    // allocate a new mem pool mgr
    pool_mgr_pt pool_mgr = malloc(sizeof(pool_mgr_t));

    // check success, on error return null
    if (pool_mgr == NULL)
        return  NULL;

    // allocate a new memory pool
    pool_mgr->pool.mem = malloc(size);
    pool_mgr->pool.policy = policy;
    pool_mgr->pool.total_size = size;

    // check success, on error deallocate mgr and return null
    if (pool_mgr->pool.mem == NULL) {
        free(pool_mgr->pool.mem);
        free(pool_mgr);
        return NULL;
    }

    // allocate a new node heap
    pool_mgr->node_heap = calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    pool_mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;

    // check success, on error deallocate mgr/pool and return null
    if (pool_mgr->node_heap == NULL) {
        free(pool_mgr->pool.mem);
        free(pool_mgr->node_heap);
        free(pool_mgr);
        return NULL;
    }

    // allocate a new gap index
    pool_mgr->gap_ix = calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    pool_mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;

    // check success, on error deallocate mgr/pool/heap and return null
    if (pool_mgr->gap_ix == NULL) {
        free(pool_mgr->pool.mem);
        free(pool_mgr->node_heap);
        free(pool_mgr->gap_ix);
        free(pool_mgr);
        return NULL;
    }

    // assign all the pointers and update meta data:

    //   initialize top node of node heap
    pool_mgr->node_heap[0].alloc_record.mem = pool_mgr->pool.mem;
    pool_mgr->node_heap[0].alloc_record.size = size;
    pool_mgr->node_heap[0].used = 1;
    pool_mgr->used_nodes = 1;
    pool_mgr->pool.num_gaps = 0;

    //   initialize top node of gap index
    _mem_add_to_gap_ix(pool_mgr, size, &pool_mgr->node_heap[0]);
    //pool_mgr->gap_ix[0].node = &pool_mgr->node_heap[0];
    //pool_mgr->gap_ix[0].node->alloc_record.size = size;

    //   link pool mgr to pool store
    pool_store[pool_store_size] = pool_mgr;
    pool_store_size++;

    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) pool_mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // check if this pool is allocated
    if (pool_mgr == NULL)
        return ALLOC_NOT_FREED;

    // check if pool has only one gap
    if (pool_mgr->pool.num_gaps != 1)
        return ALLOC_NOT_FREED;

    // check if it has zero allocations
    if (pool_mgr->pool.num_allocs != 0)
        return ALLOC_NOT_FREED;

    // free memory pool
    free(pool_mgr->pool.mem);

    // free node heap
    free(pool_mgr->node_heap);

    // free gap index
    free(pool_mgr->gap_ix);

    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    for (int i = 0; i < pool_store_size; i++) {
        if (pool_store[i] == pool_mgr)
            pool_store[i] = NULL;
    }

    // free mgr
    free(pool_mgr);

    return ALLOC_OK;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // check if any gaps, return null if none
    if (pool_mgr->pool.num_gaps == 0)
        return NULL;

    // expand heap node, if necessary, quit on error
    if (_mem_resize_node_heap(pool_mgr) != ALLOC_OK)
        return NULL;

    // check used nodes fewer than total nodes, quit on error
    if (pool_mgr->used_nodes > pool_mgr->total_nodes)
        return NULL;

    // get a node for allocation:
    node_pt node = NULL;

    // if FIRST_FIT, then find the first sufficient node in the node heap
    if (pool->policy == FIRST_FIT) {
        for (int i = 0; i < pool_mgr->total_nodes; i ++) {
            if (pool_mgr->node_heap[i].used == 1
                && pool_mgr->node_heap[i].allocated == 0
                && pool_mgr->node_heap[i].alloc_record.size >= size) {
                node = &pool_mgr->node_heap[i];
                break;
            }
        }
        // if BEST_FIT, then find the first sufficient node in the gap index
    } else if  (pool->policy == BEST_FIT) {
        for (int i = 0; i < pool_mgr->gap_ix_capacity && node == NULL; i++) {
            if (pool_mgr->gap_ix[i].size >= size)
                node = pool_mgr->gap_ix[i].node;
        }
    } else {
        return NULL;
    }

    // check if node found
    if (node == NULL) {
        return NULL;
    }

    // update metadata (num_allocs, alloc_size)
    pool->num_allocs++;
    pool->alloc_size += size;

    // calculate the size of the remaining gap, if any
    size_t remaining_gap_size = node->alloc_record.size - size;

    // remove node from gap index
    if (_mem_remove_from_gap_ix(pool_mgr, size, node) != ALLOC_OK)
        return NULL;

    // convert gap_node to an allocation node of given size
    node->allocated = 1;
    node->alloc_record.size = size;

    // adjust node heap:
    //   if remaining gap, need a new node
    if (remaining_gap_size != 0) {
        //   find an unused one in the node heap
/*        node_pt unused_node = pool_mgr->node_heap;
        while (unused_node != NULL && unused_node->used != 0) {
            unused_node = unused_node->next;
        }*/

        node_pt unused_node = NULL;
        for (int i = 0; i < pool_mgr->total_nodes; i ++) {
            if (pool_mgr->node_heap[i].used == 0) {
                unused_node = &pool_mgr->node_heap[i];
                break;
            }
        }

        //   make sure one was found
        if (unused_node == NULL)
            return NULL;

        //   initialize it to a gap node
        unused_node->allocated = 0;
        unused_node->used = 1;
        unused_node->alloc_record.size = remaining_gap_size;
        unused_node->alloc_record.mem = node->alloc_record.mem + size;

        //   update metadata (used_nodes)
        pool_mgr->used_nodes++;

        //   update linked list (new node right after the node for allocation)
        unused_node->next = node->next;
        if (node->next != NULL)  {
            node->next->prev = unused_node;
        } else {
            unused_node->next = NULL;
        }
        node->next = unused_node;
        unused_node->prev = node;

        //   add to gap index
        //   check if successful
        if (_mem_add_to_gap_ix(pool_mgr, remaining_gap_size, unused_node) != ALLOC_OK)
            return NULL;
    }

    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;

    // find the node in the node heap
    int found = 0;
    for (int i = 0; i < pool_mgr->total_nodes; i++) {
        if (&pool_mgr->node_heap[i] == node) {
            found = 1;
            break;
        }

    }

    // this is node-to-delete
    // make sure it's found
    if (!found)
        return ALLOC_FAIL;

    // convert to gap node
    node->allocated = 0;

    // update metadata (num_allocs, alloc_size)
    pool_mgr->pool.num_allocs--;
    pool_mgr->pool.alloc_size -= alloc->size;

    // if the next node in the list is also a gap, merge into node-to-delete
    if (node->next != NULL && node->next->allocated == 0) {
        //   remove the next node from gap index
        //   check success
        if (_mem_remove_from_gap_ix(pool_mgr, node->next->alloc_record.size, node->next) != ALLOC_OK)
            return ALLOC_FAIL;

        //   add the size to the node-to-delete
        node->alloc_record.size += node->next->alloc_record.size;

        //   update node as unused
        node->next->used = 0;

        //   update metadata (used nodes)
        pool_mgr->used_nodes--;

        //   update linked list:
        /*
                        if (next->next) {
                            next->next->prev = node_to_del;
                            node_to_del->next = next->next;
                        } else {
                            node_to_del->next = NULL;
                        }
                        next->next = NULL;
                        next->prev = NULL;
         */
        node_pt node_to_del = node->next;
        if (node->next->next) {
            node->next->next->prev = node;
            node->next = node->next->next;
        } else {
            node->next = NULL;
        }
        node_to_del->next = NULL;
        node_to_del->prev = NULL;
    }

    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if (node->prev != NULL && node->prev->allocated == 0) {
        //   remove the previous node from gap index
        //   check success
        if (_mem_remove_from_gap_ix(pool_mgr, node->prev->alloc_record.size, node->prev) != ALLOC_OK)
            return ALLOC_FAIL;

        //   add the size of node-to-delete to the previous
        node->prev->alloc_record.size += node->alloc_record.size;

        //   update node-to-delete as unused
        node->used = 0;

        //   update metadata (used_nodes)
        pool_mgr->used_nodes--;

        //   update linked list
        /*
                        if (node_to_del->next) {
                            prev->next = node_to_del->next;
                            node_to_del->next->prev = prev;
                        } else {
                            prev->next = NULL;
                        }
                        node_to_del->next = NULL;
                        node_to_del->prev = NULL;
         */
        node_pt prev_node = node->prev;
        if (node->next) {
            node->prev->next = node->next;
            node->next->prev = node->prev;
        } else {
            node->prev->next = NULL;
        }
        node->next = NULL;
        node->prev = NULL;
        //   change the node to add to the previous node!
        node = prev_node;
    }

    // add the resulting node to the gap index
    // check success
    if (_mem_add_to_gap_ix(pool_mgr, node->alloc_record.size, node) != ALLOC_OK)
        return ALLOC_FAIL;

    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt pool_mgr = (pool_mgr_pt) pool;

    // allocate the segments array with size == used_nodes
    pool_segment_pt seg_array = calloc(pool_mgr->used_nodes, sizeof(pool_segment_t));

    // check successful
    if (seg_array == NULL)
        return;

    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
    pool_segment_pt segment = seg_array;
    node_pt target_node = pool_mgr->node_heap;
    for (int i = 0; i < pool_mgr->used_nodes; i++) {
        segment->size = target_node->alloc_record.size;
        segment->allocated = target_node->allocated;
        segment++;
        target_node = target_node->next;
    }

    *segments = seg_array;
    *num_segments = pool_mgr->used_nodes;

}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables

    if (((float) pool_store_size / pool_store_capacity)
        > MEM_POOL_STORE_FILL_FACTOR) {
        unsigned int updated_capacity = pool_store_capacity * MEM_EXPAND_FACTOR;
        pool_store = realloc(pool_store, sizeof(pool_mgr_pt) * updated_capacity);
        if (pool_store == NULL)
            return ALLOC_FAIL;
        pool_store_capacity = updated_capacity;
    }

    return ALLOC_OK;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    // see above

    if (((float) pool_mgr->used_nodes / pool_mgr->total_nodes)
        > MEM_NODE_HEAP_FILL_FACTOR) {
        unsigned int updated_capacity = pool_mgr->total_nodes * MEM_NODE_HEAP_EXPAND_FACTOR;
        pool_mgr->node_heap = realloc(pool_mgr->node_heap, sizeof(node_t) * updated_capacity);
        if (pool_mgr->node_heap  == NULL)
            return ALLOC_FAIL;
        pool_mgr->total_nodes = updated_capacity;
    }

    return ALLOC_OK;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    // see above

    if (((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity)
        > MEM_GAP_IX_FILL_FACTOR) {
        unsigned int updated_capacity = pool_mgr->gap_ix_capacity * MEM_GAP_IX_EXPAND_FACTOR;
        pool_mgr->gap_ix = realloc(pool_mgr->gap_ix, sizeof(gap_t) * updated_capacity);
        if (pool_mgr->gap_ix  == NULL)
            return ALLOC_FAIL;
        pool_mgr->gap_ix_capacity = updated_capacity;
    }

    return ALLOC_OK;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {
    // check success
    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);

    // add the entry at the end
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = size;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = node;

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps++;

    // sort the gap index (call the function)
    if (_mem_sort_gap_ix(pool_mgr) != ALLOC_OK)
        return ALLOC_FAIL;

    return ALLOC_OK;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {

    // find the position of the node in the gap index
    int pos = -1;
    for (int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        if (pool_mgr->gap_ix[i].node == node) {
            pos = i;
            break;
        }
    }

    if (pos == -1)
        return ALLOC_FAIL;

    // loop from there to the end of the array:
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    for (int i = pos; i < pool_mgr->pool.num_gaps; i++) {
        pool_mgr->gap_ix[i].size = pool_mgr->gap_ix[i + 1].size;
        pool_mgr->gap_ix[i].node = pool_mgr->gap_ix[i + 1].node;
    }

    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps--;

    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // handle edge case of 0 or 1 sized gap index
    if (pool_mgr->pool.num_gaps < 2)
        return ALLOC_OK;

    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    for (int i = pool_mgr->pool.num_gaps - 1; i > 0; i--) {
        //    if the size of the current entry is less than the previous (u - 1)
        //    or if the sizes are the same but the current entry points to a
        //    node with a lower address of pool allocation address (mem)
        //       swap them (by copying) (remember to use a temporary variable)
        if (pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i - 1].size
            || (pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i - 1].size
                && pool_mgr->gap_ix[i].node->alloc_record.mem < pool_mgr->gap_ix[i-1].node->alloc_record.mem)) {
            gap_t temp = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i - 1];
            pool_mgr->gap_ix[i - 1] = temp;
        }
    }

    return ALLOC_OK;
}