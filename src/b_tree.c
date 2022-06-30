#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include "b_tree.h"

typedef struct tnode Tree_Node;

/* internal B+ tree struct */
typedef struct {
    /* These are the first 16/12 bytes in sector 0 */
    int key_size;
    unsigned int root_lba;
    unsigned long first_free_block;

    void * disk;            /* The jdisk */
    unsigned long size;     /* The jdisk's size */
    unsigned long num_lbas; /* size/JDISK_SECTOR_SIZE */
    int keys_per_block;     /* MAXKEY */
    int lbas_per_block;     /* MAXKEY+1 */
    Tree_Node * free_list;  /* Free list of nodes */

    Tree_Node * tmp_e; /* Pointer to external node, for When find() fails */
    int tmp_e_index;   /* and the index where the key should have gone */

    int flush; /* Whether to write sector 0 to disk */
} B_Tree;

/* internal tree node struct */
typedef struct tnode {
    /* Holds the sector for reading and writing. 
       Has space for an extra key to make splitting easier. */
    unsigned char bytes[JDISK_SECTOR_SIZE+256]; 

    unsigned char nkeys;   /* Number of keys in the node */
    unsigned char flush;   /* Whether to write sector to disk */
    unsigned char internal;/* Internal or external node */
    unsigned int lba;      /* LBA when the node is flushed */
    unsigned char **keys;  /* Pointers to the keys. Size = MAXKEY+1 */
    unsigned int *lbas;    /* Pointer to the array of LBA's. Size = MAXKEY+2 */
    struct tnode *parent;  /* Pointer to my parent -- useful for splitting */
    int parent_index;      /* My index in my parent */
    struct tnode *ptr;     /* Free list link */
} Tree_Node;

void * b_tree_create(char * filename, long size, int key_size)
{
    int i;
    unsigned char buf[JDISK_SECTOR_SIZE];

    /* setup our btree struct */
    B_Tree * btree = (B_Tree *) malloc(sizeof(B_Tree));
    btree->key_size = key_size;
    btree->root_lba = 1;
    btree->first_free_block = 2;
    btree->disk = jdisk_create(filename, size);
    btree->size = size;
    btree->num_lbas = size/JDISK_SECTOR_SIZE;
    btree->keys_per_block = (JDISK_SECTOR_SIZE - 6) / (key_size + 4);
    btree->lbas_per_block = btree->keys_per_block + 1;
    btree->free_list = NULL;
    btree->flush = 0;

    /* create btree info sector (sector 0) */
    memcpy(buf, &key_size, 4);
    memcpy(buf + 4, &btree->root_lba, 4);
    memcpy(buf + 8, &btree->first_free_block, sizeof(unsigned long));
    jdisk_write(btree->disk, 0, buf);

    /* create the root node */
    for(i = 0; i < JDISK_SECTOR_SIZE; i++) buf[i] = 0;
    jdisk_write(btree->disk, btree->root_lba, buf);

    return (void *) btree;
}

void * b_tree_attach(char * filename)
{
    B_Tree * btree;
    unsigned char buf[JDISK_SECTOR_SIZE];

    /* setup our btree struct */
    btree = (B_Tree *) malloc(sizeof(B_Tree));
    btree->disk = jdisk_attach(filename);

    jdisk_read(btree->disk, 0, buf);

    memcpy(&btree->key_size, buf, 4);
    memcpy(&btree->root_lba, buf + 4, 4);
    memcpy(&btree->first_free_block, buf + 8, sizeof(unsigned long));

    btree->size = jdisk_size(btree->disk);
    btree->num_lbas = btree->size/JDISK_SECTOR_SIZE;
    btree->keys_per_block = (JDISK_SECTOR_SIZE - 6) / (btree->key_size + 4);
    btree->lbas_per_block = btree->keys_per_block + 1;
    btree->free_list = NULL;
    btree->tmp_e = NULL;
    btree->tmp_e_index = 0;
    btree->flush = 0;

    return (void *) btree;
}

void * read_tnode(void * b_tree, unsigned int lba)
{
    B_Tree * btree;
    Tree_Node * tnode;
    int i;
    
    btree = (B_Tree *) b_tree;

    if (btree->free_list) { // nodes available on free list
        tnode = btree->free_list;
        btree->free_list = tnode->ptr;
    } else { // no nodes available, make space for a new one
        tnode = (Tree_Node *) malloc(sizeof(Tree_Node));
        tnode->keys = (unsigned char **) malloc((btree->keys_per_block + 1) *
                      sizeof(unsigned char *));
        tnode->lbas = (unsigned int *) malloc((btree->lbas_per_block + 1) * 
                      sizeof(unsigned int));

        for (i = 0; i <= btree->keys_per_block; i++)
        {
            tnode->keys[i] = tnode->bytes + 2 + (btree->key_size * i);
        }
    }

    jdisk_read(btree->disk, lba, tnode);

    memcpy(tnode->lbas,
           tnode->bytes + JDISK_SECTOR_SIZE - 
           (btree->lbas_per_block * sizeof(unsigned int)),
           (btree->lbas_per_block + 1) * sizeof(unsigned int));

    tnode->internal = tnode->bytes[0];
    tnode->nkeys = tnode->bytes[1];
    tnode->flush = 0;
    tnode->lba = lba;
    tnode->ptr = NULL;
    tnode->parent = NULL;
    tnode->parent_index = 0;

    return (void *) tnode;
}

void free_and_flush(void * t_node, void * b_tree)
{
    B_Tree * btree;
    Tree_Node * tnode;
    unsigned char buf[JDISK_SECTOR_SIZE];

    tnode = (Tree_Node *) t_node;
    btree = (B_Tree *) b_tree;

    while(tnode)
    {
        /* flush the node */
        if (tnode->flush) {
            /* write internal, nkeys, lbas (keys already written) */
            memcpy(tnode->bytes, &tnode->internal, 1);
            memcpy(tnode->bytes + 1, &tnode->nkeys, 1);
            memcpy(tnode->bytes + JDISK_SECTOR_SIZE -
                   (btree->lbas_per_block * sizeof(unsigned int)),
                   tnode->lbas,
                   (btree->lbas_per_block * sizeof(unsigned int)));

            jdisk_write(btree->disk, tnode->lba, tnode);
        }

        /* free the node */
        tnode->ptr = btree->free_list;    
        btree->free_list = tnode;

        tnode = tnode->parent;
    }

    /* flush sector 0 */
    if (btree->flush)
    {
        btree->flush = 0;

        /* write key size, lba of root node, first free sector */
        memcpy(buf, &btree->key_size, 4);
        memcpy(buf + 4, &btree->root_lba, 4);
        memcpy(buf + 8, &btree->first_free_block, sizeof(unsigned long));

        jdisk_write(btree->disk, 0, buf);
    }
}

void split_node(void * t_node, void * b_tree)
{
    B_Tree * btree;
    Tree_Node * tnode, * parent, * child;
    unsigned char buf[JDISK_SECTOR_SIZE], center;
    int i, j;

    btree = (B_Tree *) b_tree;
    tnode = (Tree_Node *) t_node;

    /* setup new child node */
    child = read_tnode(b_tree, btree->first_free_block++);

    child->flush = 1;
    child->internal = tnode->internal;

    memcpy(child->bytes, tnode->bytes, JDISK_SECTOR_SIZE);

    center = tnode->nkeys / 2;
    child->nkeys = tnode->nkeys - center - 1;

    j = 0;
    for(i = center + 1; i < tnode->nkeys; i++)
    {
        memcpy(child->keys[j], tnode->keys[i], btree->key_size);
        j++;
    }

    j = 0;
    for(i = center + 1; i < tnode->nkeys + 1; i++)
    {
        memcpy(child->lbas + j, tnode->lbas + i, sizeof(unsigned int));
        j++;
    }

    /* eliminate keys from current node */
    tnode->nkeys = center;

    /* setup the parent node */
    if (tnode->parent) { // parent already exists
        parent = tnode->parent;
        parent->flush = 1;
    } else { // parent doesn't exist, create new root
        for(i = 0; i < JDISK_SECTOR_SIZE; i++) buf[i] = 0;
        jdisk_write(btree->disk, btree->first_free_block, buf);
        btree->root_lba = btree->first_free_block;
        parent = (Tree_Node *) read_tnode(b_tree, btree->first_free_block++);
        parent->internal = 1;
        tnode->parent = parent;
        parent->flush = 1;
    }

    /* put split key in correct position, moving other keys over */
    for (i = parent->nkeys; i > tnode->parent_index; i--)
        memcpy(parent->keys[i], parent->keys[i-1], btree->key_size);

    memcpy(parent->keys[i], tnode->keys[center], btree->key_size);
    parent->nkeys++;

    /* put split lba in correct position, moving other lbas over */
    for (i = parent->nkeys; i > tnode->parent_index+1; i--)
        parent->lbas[i] = parent->lbas[i-1];

    parent->lbas[i] = child->lba;
    parent->lbas[i-1] = tnode->lba;

    free_and_flush((void *) child, b_tree);

    /* split parent if we need to */
    if (parent->nkeys > btree->keys_per_block)
        split_node((void *) parent, b_tree);
}

unsigned int b_tree_insert(void * b_tree, void * key, void * record)
{
    B_Tree * btree;
    Tree_Node * tnode; 
    unsigned int lba;
    int i;

    btree = (B_Tree *) b_tree;

    /* make sure there is enough room to insert */
    if (btree->first_free_block >= btree->num_lbas) return 0; 

    if(lba = b_tree_find(b_tree, key)) { // we found the key
        jdisk_write(btree->disk, lba, record);
    } else { // we did not find the key
        tnode = btree->tmp_e;
        tnode->flush = 1;

        /* put new key in correct positions */
        for (i = tnode->nkeys; i > btree->tmp_e_index; i--)
        {
            memcpy(tnode->keys[i], tnode->keys[i-1], btree->key_size);
        } 
        memcpy(tnode->keys[i], key, btree->key_size);

        tnode->nkeys++;

        /* put new lbas in correct positions */
        for (i = tnode->nkeys; i > btree->tmp_e_index; i--)
        {
            tnode->lbas[i] = tnode->lbas[i-1];
        }
        lba = tnode->lbas[i] = btree->first_free_block++;

        jdisk_write(btree->disk, tnode->lbas[i], record);

        /* split the node if full of keys */
        if (tnode->nkeys > btree->keys_per_block)
            split_node((void *) tnode, b_tree);

        /* flush the node(s) to disk */
        btree->flush = 1;
        free_and_flush((void *) tnode, b_tree);
    }

    return lba;
}

/* return lba of val given internal key's predecessor lba */
unsigned int get_internal_val(unsigned int lba, void * b_tree)
{
    Tree_Node * tnode; 

    tnode = (Tree_Node *) read_tnode(b_tree, lba);

    free_and_flush(tnode, b_tree);

    if (tnode->internal) // keep chasing
        get_internal_val(tnode->lbas[tnode->nkeys], b_tree);
    else // found external node
        return tnode->lbas[tnode->nkeys];
}

unsigned int b_tree_find(void * b_tree, void * key)
{
    B_Tree * btree;
    Tree_Node * tnode, * parent; 
    int low, mid, high, result;

    btree = (B_Tree *) b_tree;
    tnode = (Tree_Node *) read_tnode(b_tree, btree->root_lba);

    while (tnode->nkeys)
    {
        parent = tnode;
        low = 0;
        high = tnode->nkeys-1;

        /* binary search to find key */
        while (low <= high)
        {
            mid = (low + high) / 2;

            if ((result = memcmp(key,tnode->keys[mid],btree->key_size)) < 0)
                high = mid-1;
            else if (result > 0)
                low = mid+1;
            else { // we found the key
                free_and_flush((void *) tnode, b_tree);
                if (!tnode->internal) // external node
                    return tnode->lbas[mid];
                else // internal node, chase predecessor's right emptyval
                    return get_internal_val(tnode->lbas[mid], b_tree);
            }
        }

        /* get index where key would go in this node */
        if (memcmp(key, tnode->keys[mid], btree->key_size) > 0) mid++;

        if (!tnode->internal) { // did not find key
            btree->tmp_e = tnode;
            btree->tmp_e_index = mid;
            return 0;
        } else { // keep chasing
            tnode = (Tree_Node *) read_tnode(b_tree, tnode->lbas[mid]);
            tnode->parent_index = mid;
        }

        tnode->parent = parent;
    }

    btree->tmp_e = tnode;
    btree->tmp_e_index = 0;

    return 0;
}

void * b_tree_disk(void * b_tree)
{
    return ((B_Tree *) b_tree)->disk;
}

int b_tree_key_size(void * b_tree)
{
    return ((B_Tree *) b_tree)->key_size;
}

/* breadth first print all nodes in tree */
void b_tree_print_tree(void * b_tree)
{
    B_Tree * btree;
    Tree_Node * tnode; 
    int i;

    btree = (B_Tree *) b_tree;
    btree->tmp_e = (Tree_Node *) read_tnode(b_tree, btree->root_lba);

    while (btree->tmp_e)
    {
        /* print node */
        printf("LBA 0x%08x.  Internal: %d\n",
               btree->tmp_e->lba,
               btree->tmp_e->internal);

        for (i = 0; i <= btree->tmp_e->nkeys; i++)
        {
            printf("  Entry %d:", i);

            if (i != btree->tmp_e->nkeys)
                printf(" Key: %-32s", btree->tmp_e->keys[i]);
            else
                printf("%-38s","");

            printf(" LBA: 0x%08x\n",btree->tmp_e->lbas[i]);
        }
        printf("\n");

        /* add child nodes to queue */
        if (btree->tmp_e->internal) {
            tnode = btree->tmp_e;
            while (tnode->ptr) tnode = tnode->ptr;
            for (i = 0; i < btree->tmp_e->nkeys + 1; i++)
            {
                tnode->ptr = (Tree_Node *)
                             read_tnode(b_tree, btree->tmp_e->lbas[i]);
                tnode = tnode->ptr;
            }
        }

        tnode = btree->tmp_e->ptr;

        /* free the already printed node */
        btree->tmp_e->ptr = btree->free_list;    
        btree->free_list = btree->tmp_e;

        btree->tmp_e = tnode;
    }
}
