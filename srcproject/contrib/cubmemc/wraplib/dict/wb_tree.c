/*
 * wb_tree.c
 *
 * Implementation of weight balanced tree.
 * Copyright (C) 2001 Farooq Mela.
 *
 * $Id: wb_tree.c,v 1.13 2002/07/16 00:50:16 farooq Exp $
 *
 * cf. [Gonnet 1984], [Nievergelt and Reingold 1973]
 */

#include <stdlib.h>
#include <limits.h>

#include "wb_tree.h"
#include "dict_private.h"

/* A tree BB[alpha] is said to be of weighted balance alpha if every node in
 * the tree has a balance p(n) such that alpha <= p(n) <= 1 - alpha. The
 * balance of a node is defined as the number of nodes in its left subtree
 * divided by the number of nodes in either subtree. The weight of a node is
 * defined as the number of external nodes in its subtrees.
 *
 * Legal values for alpha are 0 <= alpha <= 1/2. BB[0] is a normal, unbalanced
 * binary tree, and BB[1/2] includes only completely balanced binary search
 * trees of 2^height - 1 nodes. A higher value of alpha specifies a more
 * stringent balance requirement. Values for alpha in the range 2/11 <= alpha
 * <= 1 - sqrt(2)/2 are interesting because a tree can be brought back into
 * weighted balance after an insertion or deletion using at most one rotation
 * per level (thus the number of rotations after insertion or deletion is
 * O(lg N)).
 *
 * These are the parameters for alpha = 1 - sqrt(2)/2 == .292893
 * These are the values recommended in [Gonnet 1984]. The author of this
 * library has experimented with these values to some extent and found that
 * this value of alpha typically gives the best performance. */

#define ALPHA_0		.292893f	/* 1 - sqrt(2)/2		*/
#define ALPHA_1		.707106f	/* 1 - (1 - sqrt(2)/2)	*/
#define ALPHA_2		.414213f	/* sqrt(2) - 1			*/
#define ALPHA_3		.585786f	/* 1 - (sqrt(2) - 1)	*/

#if UINT_MAX == 0xffffffffUL
typedef unsigned int	weight_t;
#else
typedef unsigned long	weight_t;
#endif

typedef struct wb_node wb_node;
struct wb_node {
	void		*key;
	void		*dat;
	wb_node		*parent;
	wb_node		*llink;
	wb_node		*rlink;
	weight_t	 weight;
};

#define WEIGHT(n)	((n) ? (n)->weight : 1)
#define REWEIGH(n)	(n)->weight = WEIGHT((n)->llink) + WEIGHT((n)->rlink)

struct wb_tree {
	wb_node			*root;
	unsigned		 count;
	dict_cmp_func	 key_cmp;
	dict_del_func	 key_del;
	dict_del_func	 dat_del;
};

struct wb_itor {
	wb_tree	*tree;
	wb_node	*node;
};

static void rot_left __P((wb_tree *tree, wb_node *node));
static void rot_right __P((wb_tree *tree, wb_node *node));
static unsigned node_height __P((const wb_node *node));
static unsigned node_mheight __P((const wb_node *node));
static unsigned node_pathlen __P((const wb_node *node, unsigned level));
static wb_node *node_new __P((void *key, void *dat));
static wb_node *node_min __P((wb_node *node));
static wb_node *node_max __P((wb_node *node));
static wb_node *node_next __P((wb_node *node));
static wb_node *node_prev __P((wb_node *node));

wb_tree *
wb_tree_new(dict_cmp_func key_cmp, dict_del_func key_del,
			dict_del_func dat_del)
{
	wb_tree *tree;

	if ((tree = MALLOC(sizeof(*tree))) == NULL)
		return NULL;

	tree->root = NULL;
	tree->count = 0;
	tree->key_cmp = key_cmp ? key_cmp : dict_ptr_cmp;
	tree->key_del = key_del;
	tree->dat_del = dat_del;

	return tree;
}

dict *
wb_dict_new(dict_cmp_func key_cmp, dict_del_func key_del,
			dict_del_func dat_del)
{
	dict *dct;
	wb_tree *tree;

	if ((dct = MALLOC(sizeof(*dct))) == NULL)
		return NULL;

	if ((tree = wb_tree_new(key_cmp, key_del, dat_del)) == NULL) {
		FREE(dct);
		return NULL;
	}

	dct->_object = tree;
	dct->_inew = (inew_func)wb_dict_itor_new;
	dct->_destroy = (destroy_func)wb_tree_destroy;
	dct->_insert = (insert_func)wb_tree_insert;
	dct->_probe = (probe_func)wb_tree_probe;
	dct->_search = (search_func)wb_tree_search;
	dct->_csearch = (csearch_func)wb_tree_csearch;
	dct->_remove = (remove_func)wb_tree_remove;
	dct->_empty = (empty_func)wb_tree_empty;
	dct->_walk = (walk_func)wb_tree_walk;
	dct->_count = (count_func)wb_tree_count;

	return dct;
}

void
wb_tree_destroy(wb_tree *tree, int del)
{
	ASSERT(tree != NULL);

	if (tree->root)
		wb_tree_empty(tree, del);

	FREE(tree);
}

void *
wb_tree_search(wb_tree *tree, const void *key)
{
	int rv;
	wb_node *node;

	ASSERT(tree != NULL);

	node = tree->root;
	while (node) {
		rv = tree->key_cmp(key, node->key);
		if (rv < 0)
			node = node->llink;
		else if (rv > 0)
			node = node->rlink;
		else
			return node->dat;
	}

	return NULL;
}

const void *
wb_tree_csearch(const wb_tree *tree, const void *key)
{
	return wb_tree_search((wb_tree *)tree, key);
}

int
wb_tree_insert(wb_tree *tree, void *key, void *dat, int overwrite)
{
	int rv = 0;
	wb_node *node, *parent = NULL;
	float wbal;

	ASSERT(tree != NULL);

	node = tree->root;
	while (node) {
		rv = tree->key_cmp(key, node->key);
		if (rv < 0)
			parent = node, node = node->llink;
		else if (rv > 0)
			parent = node, node = node->rlink;
		else {
			if (overwrite == 0)
				return 1;
			if (tree->key_del)
				tree->key_del(node->key);
			if (tree->dat_del)
				tree->dat_del(node->dat);
			node->key = key;
			node->dat = dat;
			return 0;
		}
	}

	if ((node = node_new(key, dat)) == NULL)
		return -1;
	if ((node->parent = parent) == NULL) {
		ASSERT(tree->count == 0);
		tree->root = node;
		tree->count = 1;
		return 0;
	}
	if (rv < 0)
		parent->llink = node;
	else
		parent->rlink = node;

	while ((node = parent) != NULL) {
		parent = node->parent;
		node->weight++;
		wbal = WEIGHT(node->llink) / (float)node->weight;
		if (wbal < ALPHA_0) {
			wbal = WEIGHT(node->rlink->llink) / (float)node->rlink->weight;
			if (wbal < ALPHA_3) {		/* LL */
				rot_left(tree, node);
			} else {					/* RL */
				rot_right(tree, node->rlink);
				rot_left(tree, node);
			}
		} else if (wbal > ALPHA_1) {
			wbal = WEIGHT(node->llink->llink) / (float)node->llink->weight;
			if (wbal > ALPHA_2) {		/* RR */
				rot_right(tree, node);
			} else {					/* LR */
				rot_left(tree, node->llink);
				rot_right(tree, node);
			}
		}
	}
	tree->count++;
	return 0;
}

int
wb_tree_probe(wb_tree *tree, void *key, void **dat)
{
	int rv = 0;
	wb_node *node, *parent = NULL;
	float wbal;

	ASSERT(tree != NULL);

	node = tree->root;
	while (node) {
		rv = tree->key_cmp(key, node->key);
		if (rv < 0)
			parent = node, node = node->llink;
		else if (rv > 0)
			parent = node, node = node->rlink;
		else {
			*dat = node->dat;
			return 0;
		}
	}

	if ((node = node_new(key, *dat)) == NULL)
		return -1;
	if ((node->parent = parent) == NULL) {
		ASSERT(tree->count == 0);
		tree->root = node;
		tree->count = 1;
		return 0;
	}
	if (rv < 0)
		parent->llink = node;
	else
		parent->rlink = node;

	while ((node = parent) != NULL) {
		parent = node->parent;
		node->weight++;
		wbal = WEIGHT(node->llink) / (float)node->weight;
		if (wbal < ALPHA_0) {
			wbal = WEIGHT(node->rlink->llink) / (float)node->rlink->weight;
			if (wbal < ALPHA_3) {
				rot_left(tree, node);
			} else {
				rot_right(tree, node->rlink);
				rot_left(tree, node);
			}
		} else if (wbal > ALPHA_1) {
			wbal = WEIGHT(node->llink->llink) / (float)node->llink->weight;
			if (wbal > ALPHA_2) {
				rot_right(tree, node);
			} else {
				rot_left(tree, node->llink);
				rot_right(tree, node);
			}
		}
	}
	tree->count++;
	return 1;
}

int
wb_tree_remove(wb_tree *tree, const void *key, int del)
{
	int rv;
	wb_node *node, *temp, *out = NULL; /* ergh @ GCC unitializated warning */

	ASSERT(tree != NULL);
	ASSERT(key != NULL);

	node = tree->root;
	while (node) {
		rv = tree->key_cmp(key, node->key);
		if (rv) {
			node = rv < 0 ? node->llink : node->rlink;
			continue;
		}
		if (node->llink == NULL) {
			temp = node;
			out = node->rlink;
			if (out)
				out->parent = node->parent;
			if (del) {
				if (tree->key_del)
					tree->key_del(node->key);
				if (tree->dat_del)
					tree->dat_del(node->dat);
			}
			if (node->parent) {
				if (node->parent->llink == node)
					node->parent->llink = out;
				else
					node->parent->rlink = out;
			} else {
				tree->root = out;
			}
			FREE(node);
			out = temp;
		} else if (node->rlink == NULL) {
			temp = node;
			out = node->llink;
			if (out)
				out->parent = node->parent;
			if (del) {
				if (tree->key_del)
					tree->key_del(node->key);
				if (tree->dat_del)
					tree->dat_del(node->dat);
			}
			if (node->parent) {
				if (node->parent->llink == node)
					node->parent->llink = out;
				else
					node->parent->rlink = out;
			} else {
				tree->root = out;
			}
			FREE(node);
			out = temp;
		} else if (WEIGHT(node->llink) > WEIGHT(node->rlink)) {
			if (WEIGHT(node->llink->llink) < WEIGHT(node->llink->rlink))
				rot_left(tree, node->llink);
			out = node->llink;
			rot_right(tree, node);
			node = out->rlink;
			continue;
		} else {
			if (WEIGHT(node->rlink->rlink) < WEIGHT(node->rlink->llink))
				rot_right(tree, node->rlink);
			out = node->rlink;
			rot_left(tree, node);
			node = out->llink;
			continue;
		}

		if (--tree->count) {
			while (out) {
				out->weight--;
				out = out->parent;
			}
		}
		return 0;
	}
	return -1;
}

void
wb_tree_empty(wb_tree *tree, int del)
{
	wb_node *node, *parent;

	ASSERT(tree != NULL);

	node = tree->root;

	while (node) {
		if (node->llink || node->rlink) {
			node = node->llink ? node->llink : node->rlink;
			continue;
		}

		if (del) {
			if (tree->key_del)
				tree->key_del(node->key);
			if (tree->dat_del)
				tree->dat_del(node->dat);
		}

		parent = node->parent;
		FREE(node);

		if (parent) {
			if (parent->llink == node)
				parent->llink = NULL;
			else
				parent->rlink = NULL;
		}
		node = parent;
	}

	tree->root = NULL;
	tree->count = 0;
}

const void *
wb_tree_min(const wb_tree *tree)
{
	const wb_node *node;

	ASSERT(tree != NULL);

	if ((node = tree->root) == NULL)
		return NULL;

	for (; node->llink; node = node->llink)
		/* void */;
	return node->key;
}

const void *
wb_tree_max(const wb_tree *tree)
{
	const wb_node *node;

	ASSERT(tree != NULL);

	if (tree->root == NULL)
		return NULL;

	for (node = tree->root; node->rlink; node = node->rlink)
		/* void */;
	return node->key;
}

void
wb_tree_walk(wb_tree *tree, dict_vis_func visit)
{
	wb_node *node;

	ASSERT(tree != NULL);

	if (tree->root == NULL)
		return;
	for (node = node_min(tree->root); node; node = node_next(node))
		if (visit(node->key, node->dat) == 0)
			break;
}

unsigned
wb_tree_count(const wb_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->count;
}

unsigned
wb_tree_height(const wb_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_height(tree->root) : 0;
}

unsigned
wb_tree_mheight(const wb_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_mheight(tree->root) : 0;
}

unsigned
wb_tree_pathlen(const wb_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_pathlen(tree->root, 1) : 0;
}

static wb_node *
node_new(void *key, void *dat)
{
	wb_node *node;

	if ((node = MALLOC(sizeof(*node))) == NULL)
		return NULL;

	node->key = key;
	node->dat = dat;
	node->parent = NULL;
	node->llink = NULL;
	node->rlink = NULL;
	node->weight = 2;

	return node;
}

static wb_node *
node_min(wb_node *node)
{
	ASSERT(node != NULL);

	while (node->llink)
		node = node->llink;
	return node;
}

static wb_node *
node_max(wb_node *node)
{
	ASSERT(node != NULL);

	while (node->rlink)
		node = node->rlink;
	return node;
}

static wb_node *
node_next(wb_node *node)
{
	wb_node *temp;

	ASSERT(node != NULL);

	if (node->rlink) {
		for (node = node->rlink; node->llink; node = node->llink)
			/* void */;
		return node;
	}
	temp = node->parent;
	while (temp && temp->rlink == node) {
		node = temp;
		temp = temp->parent;
	}
	return temp;
}

static wb_node *
node_prev(wb_node *node)
{
	wb_node *temp;

	ASSERT(node != NULL);

	if (node->llink) {
		for (node = node->llink; node->rlink; node = node->rlink)
			/* void */;
		return node;
	}
	temp = node->parent;
	while (temp && temp->llink == node) {
		node = temp;
		temp = temp->parent;
	}
	return temp;
}

static unsigned
node_height(const wb_node *node)
{
	unsigned l, r;

	ASSERT(node != NULL);

	l = node->llink ? node_height(node->llink) + 1 : 0;
	r = node->rlink ? node_height(node->rlink) + 1 : 0;
	return MAX(l, r);
}

static unsigned
node_mheight(const wb_node *node)
{
	unsigned l, r;

	ASSERT(node != NULL);

	l = node->llink ? node_mheight(node->llink) + 1 : 0;
	r = node->rlink ? node_mheight(node->rlink) + 1 : 0;
	return MIN(l, r);
}

static unsigned
node_pathlen(const wb_node *node, unsigned level)
{
	unsigned n = 0;

	ASSERT(node != NULL);

	if (node->llink)
		n += level + node_pathlen(node->llink, level + 1);
	if (node->rlink)
		n += level + node_pathlen(node->rlink, level + 1);
	return n;
}

/*
 * rot_left(T, B):
 *
 *     /             /
 *    B             D
 *   / \           / \
 *  A   D   ==>   B   E
 *     / \       / \
 *    C   E     A   C
 *
 * Only the weights of B and RIGHT(B) need to be readjusted, because upper
 * level nodes' weights haven't changed.
 */
static void
rot_left(wb_tree *tree, wb_node *node)
{
	wb_node *rlink, *parent;

	ASSERT(tree != NULL);
	ASSERT(node != NULL);
	ASSERT(node->rlink != NULL);

	rlink = node->rlink;
	node->rlink = rlink->llink;
	if (rlink->llink)
		rlink->llink->parent = node;
	parent = node->parent;
	rlink->parent = parent;
	if (parent) {
		if (parent->llink == node)
			parent->llink = rlink;
		else
			parent->rlink = rlink;
	} else {
		tree->root = rlink;
	}
	rlink->llink = node;
	node->parent = rlink;

	REWEIGH(node);
	REWEIGH(rlink);
}

/*
 * rot_right(T, D):
 *
 *       /           /
 *      D           B
 *     / \         / \
 *    B   E  ==>  A   D
 *   / \             / \
 *  A   C           C   E
 *
 * Only the weights of D and LEFT(D) need to be readjusted, because upper level
 * nodes' weights haven't changed.
 */
static void
rot_right(wb_tree *tree, wb_node *node)
{
	wb_node *llink, *parent;

	ASSERT(tree != NULL);
	ASSERT(node != NULL);
	ASSERT(node->llink != NULL);

	llink = node->llink;
	node->llink = llink->rlink;
	if (llink->rlink)
		llink->rlink->parent = node;
	parent = node->parent;
	llink->parent = parent;
	if (parent) {
		if (parent->llink == node)
			parent->llink = llink;
		else
			parent->rlink = llink;
	} else {
		tree->root = llink;
	}
	llink->rlink = node;
	node->parent = llink;

	REWEIGH(node);
	REWEIGH(llink);
}

wb_itor *
wb_itor_new(wb_tree *tree)
{
	wb_itor *itor;

	ASSERT(tree != NULL);

	itor = MALLOC(sizeof(*itor));
	if (itor) {
		itor->tree = tree;
		wb_itor_first(itor);
	}
	return itor;
}

dict_itor *
wb_dict_itor_new(wb_tree *tree)
{
	dict_itor *itor;

	ASSERT(tree != NULL);

	if ((itor = MALLOC(sizeof(*itor))) == NULL)
		return NULL;

	if ((itor->_itor = wb_itor_new(tree)) == NULL) {
		FREE(itor);
		return NULL;
	}

	itor->_destroy = (idestroy_func)wb_itor_destroy;
	itor->_valid = (valid_func)wb_itor_valid;
	itor->_invalid = (invalidate_func)wb_itor_invalidate;
	itor->_next = (next_func)wb_itor_next;
	itor->_prev = (prev_func)wb_itor_prev;
	itor->_nextn = (nextn_func)wb_itor_nextn;
	itor->_prevn = (prevn_func)wb_itor_prevn;
	itor->_first = (first_func)wb_itor_first;
	itor->_last = (last_func)wb_itor_last;
	itor->_search = (isearch_func)wb_itor_search;
	itor->_key = (key_func)wb_itor_key;
	itor->_data = (data_func)wb_itor_data;
	itor->_cdata = (cdata_func)wb_itor_cdata;
	itor->_setdata = (dataset_func)wb_itor_set_data;

	return itor;
}

void
wb_itor_destroy(wb_itor *itor)
{
	ASSERT(itor != NULL);

	FREE(itor);
}

#define RETVALID(itor)		return itor->node != NULL

int
wb_itor_valid(const wb_itor *itor)
{
	ASSERT(itor != NULL);

	RETVALID(itor);
}

void
wb_itor_invalidate(wb_itor *itor)
{
	ASSERT(itor != NULL);

	itor->node = NULL;
}

int
wb_itor_next(wb_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		wb_itor_first(itor);
	else
		itor->node = node_next(itor->node);
	RETVALID(itor);
}

int
wb_itor_prev(wb_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		wb_itor_last(itor);
	else
		itor->node = node_prev(itor->node);
	RETVALID(itor);
}

int
wb_itor_nextn(wb_itor *itor, unsigned count)
{
	ASSERT(itor != NULL);

	if (count) {
		if (itor->node == NULL) {
			wb_itor_first(itor);
			count--;
		}

		while (count-- && itor->node)
			itor->node = node_next(itor->node);
	}

	RETVALID(itor);
}

int
wb_itor_prevn(wb_itor *itor, unsigned count)
{
	ASSERT(itor != NULL);

	if (count) {
		if (itor->node == NULL) {
			wb_itor_last(itor);
			count--;
		}

		while (count-- && itor->node)
			itor->node = node_prev(itor->node);
	}

	RETVALID(itor);
}

int
wb_itor_first(wb_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->tree->root == NULL)
		itor->node = NULL;
	else
		itor->node = node_min(itor->tree->root);
	RETVALID(itor);
}

int
wb_itor_last(itor)
	wb_itor *itor;
{
	ASSERT(itor != NULL);

	if (itor->tree->root == NULL)
		itor->node = NULL;
	else
		itor->node = node_max(itor->tree->root);
	RETVALID(itor);
}

int
wb_itor_search(wb_itor *itor, const void *key)
{
	int rv;
	wb_node *node;
	dict_cmp_func cmp;

	ASSERT(itor != NULL);

	cmp = itor->tree->key_cmp;
	for (node = itor->tree->root; node;) {
		rv = cmp(key, node->key);
		if (rv < 0)
			node = node->llink;
		else if (rv > 0)
			node = node->rlink;
		else {
			itor->node = node;
			return TRUE;
		}
	}
	itor->node = NULL;
	return FALSE;
}

const void *
wb_itor_key(const wb_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->key : NULL;
}

void *
wb_itor_data(wb_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->dat : NULL;
}

const void *
wb_itor_cdata(const wb_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->dat : NULL;
}

int
wb_itor_set_data(wb_itor *itor, void *dat, int del)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		return -1;

	if (del && itor->tree->dat_del)
		itor->tree->dat_del(itor->node->dat);
	itor->node->dat = dat;
	return 0;
}
