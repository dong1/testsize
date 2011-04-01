/*
 * tr_tree.c
 *
 * Implementation of treap.
 * Copyright (C) 2001 Farooq Mela.
 *
 * $Id: tr_tree.c,v 1.14 2001/12/17 08:12:40 farooq Exp $
 *
 * cf. [Aragon and Seidel, 1996], [Knuth 1998]
 *
 * A treap is a randomized data structure in which each node of tree has an
 * associated key and priority. The priority is chosen at random when the node
 * is inserted into the tree. Each node is inserted so that the lexicographic
 * order of the keys is preserved, and the priority of any node is less than
 * the priority of either of its child nodes; in this way the treap is a
 * combination of a tree and a min-heap. In this implementation, this is
 * accomplished by first inserting the node according to lexigraphical order of
 * keys as in a normal binary tree, and then, if needed, sifting the node
 * upwards using a series of rotations until the heap property of the tree is
 * restored.
 */

#include <stdlib.h>
#include <limits.h>

#include "tr_tree.h"
#include "dict_private.h"

/* We want a priority_t to be a 32-bit value.
 * I_{j+1} = I_j * A + M; A suggested by Knuth, M by H.W. Lewis. */
#if UINT_MAX == 0xffffffff
typedef unsigned int priority_t;
#define RGEN_A		1664525U
#define RGEN_M		1013904223U
#define RGEN_MASK	0xffffffffU
#else
typedef unsigned long priority_t;
#define RGEN_A		1664525UL
#define RGEN_M		1013904223UL
#define RGEN_MASK	0xffffffffUL
#endif

typedef struct tr_node tr_node;
struct tr_node {
	void		*key;
	void		*dat;
	tr_node		*parent;
	tr_node		*llink;
	tr_node		*rlink;
	priority_t	 prio;
};

struct tr_tree {
	tr_node			*root;
	unsigned		 count;
	dict_cmp_func	 key_cmp;
	dict_del_func	 key_del;
	dict_del_func	 dat_del;
	unsigned long	 randgen;
};

struct tr_itor {
	tr_tree	*tree;
	tr_node	*node;
};

static void rot_left __P((tr_tree *tree, tr_node *node));
static void rot_right __P((tr_tree *tree, tr_node *node));
static unsigned node_height __P((const tr_node *node));
static unsigned node_mheight __P((const tr_node *node));
static unsigned node_pathlen __P((const tr_node *node, unsigned level));
static tr_node *node_new __P((void *key, void *dat));
static tr_node *node_next __P((tr_node *node));
static tr_node *node_prev __P((tr_node *node));
static tr_node *node_max __P((tr_node *node));
static tr_node *node_min __P((tr_node *node));

tr_tree *
tr_tree_new(dict_cmp_func key_cmp, dict_del_func key_del,
			dict_del_func dat_del)
{
	tr_tree *tree;

	if ((tree = MALLOC(sizeof(*tree))) == NULL)
		return NULL;

	tree->root = NULL;
	tree->count = 0;
	tree->key_cmp = key_cmp ? key_cmp : dict_ptr_cmp;
	tree->key_del = key_del;
	tree->dat_del = dat_del;
	tree->randgen = rand();

	return tree;
}

dict *
tr_dict_new(dict_cmp_func key_cmp, dict_del_func key_del,
			dict_del_func dat_del)
{
	dict *dct;
	tr_tree *tree;

	if ((dct = MALLOC(sizeof(*dct))) == NULL)
		return NULL;

	if ((tree = tr_tree_new(key_cmp, key_del, dat_del)) == NULL) {
		FREE(dct);
		return NULL;
	}

	dct->_object = tree;
	dct->_inew = (inew_func)tr_dict_itor_new;
	dct->_destroy = (destroy_func)tr_tree_destroy;
	dct->_insert = (insert_func)tr_tree_insert;
	dct->_probe = (probe_func)tr_tree_probe;
	dct->_search = (search_func)tr_tree_search;
	dct->_csearch = (csearch_func)tr_tree_csearch;
	dct->_remove = (remove_func)tr_tree_remove;
	dct->_empty = (empty_func)tr_tree_empty;
	dct->_walk = (walk_func)tr_tree_walk;
	dct->_count = (count_func)tr_tree_count;

	return dct;
}

void
tr_tree_destroy(tr_tree *tree, int del)
{
	ASSERT(tree != NULL);

	if (tree->root)
		tr_tree_empty(tree, del);
	FREE(tree);
}

void
tr_tree_empty(tr_tree *tree, int del)
{
	tr_node *node, *parent;

	ASSERT(tree != NULL);

	node = tree->root;
	while (node) {
		parent = node->parent;
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

int
tr_tree_insert(tr_tree *tree, void *key, void *dat, int overwrite)
{
	int rv = 0;
	tr_node *node, *parent = NULL;
	priority_t prio;

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
	prio = (tree->randgen * RGEN_A + RGEN_M) & RGEN_MASK;
	node->prio = tree->randgen = prio;

	if ((node->parent = parent) == NULL) {
		tree->root = node;
		ASSERT(tree->count == 0);
		tree->count = 1;
		return 0;
	}
	if (rv < 0)
		parent->llink = node;
	else
		parent->rlink = node;

	do {
		if (parent->prio <= node->prio)
			break;
		if (parent->llink == node)
			rot_right(tree, parent);
		else
			rot_left(tree, parent);
		parent = node->parent;
	} while (parent);

	tree->count++;
	return 0;
}

int
tr_tree_probe(tr_tree *tree, void *key, void **dat)
{
	int rv = 0;
	tr_node *node, *parent = NULL;
	priority_t prio;

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
	prio = (tree->randgen * RGEN_A + RGEN_M) & RGEN_MASK;
	node->prio = tree->randgen = prio;

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

	do {
		if (parent->prio <= node->prio)
			break;
		if (parent->llink == node)
			rot_right(tree, parent);
		else
			rot_left(tree, parent);
		parent = node->parent;
	} while (parent);

	tree->count++;
	return 0;
}

int
tr_tree_remove(tr_tree *tree, const void *key, int del)
{
	int rv;
	tr_node *node, *out, *parent = NULL;

	ASSERT(tree != NULL);

	node = tree->root;
	while (node) {
		rv = tree->key_cmp(key, node->key);
		if (rv < 0)
			parent = node, node = node->llink;
		else if (rv > 0)
			parent = node, node = node->rlink;
		else
			break;
	}

	if (node == NULL)
		return -1;

	while (node->llink && node->rlink) {
		if (node->llink->prio < node->rlink->prio)
			rot_right(tree, node);
		else
			rot_left(tree, node);
	}
	parent = node->parent;
	out = node->llink ? node->llink : node->rlink;
	if (out)
		out->parent = parent;
	if (parent) {
		if (parent->llink == node)
			parent->llink = out;
		else
			parent->rlink = out;
	} else {
		tree->root = out;
	}

	if (del) {
		if (tree->key_del)
			tree->key_del(node->key);
		if (tree->dat_del)
			tree->dat_del(node->dat);
	}
	FREE(node);

	tree->count--;
	return 0;
}

void *
tr_tree_search(tr_tree *tree, const void *key)
{
	int rv;
	tr_node *node;
	dict_cmp_func cmp;

	ASSERT(tree != NULL);

	node = tree->root;
	cmp = tree->key_cmp;
	while (node) {
		rv = cmp(key, node->key);
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
tr_tree_csearch(const tr_tree *tree, const void *key)
{
	ASSERT(tree != NULL);

	return tr_tree_search((tr_tree *)tree, key);
}

void
tr_tree_walk(tr_tree *tree, dict_vis_func visit)
{
	tr_node *node;

	ASSERT(tree != NULL);
	ASSERT(visit != NULL);

	if (tree->root == NULL)
		return;

	for (node = node_min(tree->root); node; node = node_next(node))
		if (visit(node->key, node->dat) == 0)
			break;
}

unsigned
tr_tree_count(const tr_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->count;
}

unsigned
tr_tree_height(const tr_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_height(tree->root) : 0;
}

unsigned
tr_tree_mheight(const tr_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_mheight(tree->root) : 0;
}

unsigned
tr_tree_pathlen(const tr_tree *tree)
{
	ASSERT(tree != NULL);

	return tree->root ? node_pathlen(tree->root, 1) : 0;
}

const void *
tr_tree_min(const tr_tree *tree)
{
	const tr_node *node;

	ASSERT(tree != NULL);

	if ((node = tree->root) == NULL)
		return NULL;

	for (; node->llink; node = node->llink)
		/* void */;
	return node->key;
}

const void *
tr_tree_max(const tr_tree *tree)
{
	const tr_node *node;

	ASSERT(tree != NULL);

	if ((node = tree->root) == NULL)
		return NULL;

	for (; node->rlink; node = node->rlink)
		/* void */;
	return node->key;
}

static void
rot_left(tr_tree *tree, tr_node *node)
{
	tr_node *rlink, *parent;

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
}

static void
rot_right(tr_tree *tree, tr_node *node)
{
	tr_node *llink, *parent;

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
}

static tr_node *
node_new(void *key, void *dat)
{
	tr_node *node;

	if ((node = MALLOC(sizeof(*node))) == NULL)
		return NULL;

	node->key = key;
	node->dat = dat;
	node->parent = NULL;
	node->llink = NULL;
	node->rlink = NULL;

	return node;
}

static tr_node *
node_next(tr_node *node)
{
	tr_node *temp;

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

static tr_node *
node_prev(tr_node *node)
{
	tr_node *temp;

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

static tr_node *
node_max(tr_node *node)
{
	ASSERT(node != NULL);

	while (node->rlink)
		node = node->rlink;
	return node;
}

static tr_node *
node_min(tr_node *node)
{
	ASSERT(node != NULL);

	while (node->llink)
		node = node->llink;
	return node;
}

static unsigned
node_height(const tr_node *node)
{
	unsigned l, r;

	ASSERT(node != NULL);

	l = node->llink ? node_height(node->llink) + 1 : 0;
	r = node->rlink ? node_height(node->rlink) + 1 : 0;
	return MAX(l, r);
}

static unsigned
node_mheight(const tr_node *node)
{
	unsigned l, r;

	ASSERT(node != NULL);

	l = node->llink ? node_mheight(node->llink) + 1 : 0;
	r = node->rlink ? node_mheight(node->rlink) + 1 : 0;
	return MIN(l, r);
}

static unsigned
node_pathlen(const tr_node *node, unsigned level)
{
	unsigned n = 0;

	ASSERT(node != NULL);

	if (node->llink)
		n += level + node_pathlen(node->llink, level + 1);
	if (node->rlink)
		n += level + node_pathlen(node->rlink, level + 1);
	return n;
}

tr_itor *
tr_itor_new(tr_tree *tree)
{
	tr_itor *itor;

	ASSERT(tree != NULL);

	itor = MALLOC(sizeof(*itor));
	if (itor) {
		itor->tree = tree;
		tr_itor_first(itor);
	}
	return itor;
}

dict_itor *
tr_dict_itor_new(tr_tree *tree)
{
	dict_itor *itor;

	ASSERT(tree != NULL);

	itor = MALLOC(sizeof(*itor));
	if (itor == NULL)
		return NULL;

	if ((itor->_itor = tr_itor_new(tree)) == NULL) {
		FREE(itor);
		return NULL;
	}

	itor->_destroy = (idestroy_func)tr_itor_destroy;
	itor->_valid = (valid_func)tr_itor_valid;
	itor->_invalid = (invalidate_func)tr_itor_invalidate;
	itor->_next = (next_func)tr_itor_next;
	itor->_prev = (prev_func)tr_itor_prev;
	itor->_nextn = (nextn_func)tr_itor_nextn;
	itor->_prevn = (prevn_func)tr_itor_prevn;
	itor->_first = (first_func)tr_itor_first;
	itor->_last = (last_func)tr_itor_last;
	itor->_search = (isearch_func)tr_itor_search;
	itor->_key = (key_func)tr_itor_key;
	itor->_data = (data_func)tr_itor_data;
	itor->_cdata = (cdata_func)tr_itor_cdata;
	itor->_setdata = (dataset_func)tr_itor_set_data;

	return itor;
}

void
tr_itor_destroy(tr_itor *itor)
{
	ASSERT(itor != NULL);

	FREE(itor);
}

#define RETVALID(itor)		return itor->node != NULL

int
tr_itor_valid(const tr_itor *itor)
{
	ASSERT(itor != NULL);

	RETVALID(itor);
}

void
tr_itor_invalidate(tr_itor *itor)
{
	ASSERT(itor != NULL);

	itor->node = NULL;
}

int
tr_itor_next(tr_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		tr_itor_first(itor);
	else
		itor->node = node_next(itor->node);
	RETVALID(itor);
}

int
tr_itor_prev(tr_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		tr_itor_last(itor);
	else
		itor->node = node_prev(itor->node);
	RETVALID(itor);
}

int
tr_itor_nextn(tr_itor *itor, unsigned count)
{
	ASSERT(itor != NULL);

	if (count) {
		if (itor->node == NULL) {
			tr_itor_first(itor);
			count--;
		}

		while (count-- && itor->node)
			itor->node = node_next(itor->node);
	}

	RETVALID(itor);
}

int
tr_itor_prevn(tr_itor *itor, unsigned count)
{
	ASSERT(itor != NULL);

	if (count) {
		if (itor->node == NULL) {
			tr_itor_last(itor);
			count--;
		}

		while (count-- && itor->node)
			itor->node = node_prev(itor->node);
	}

	RETVALID(itor);
}

int
tr_itor_first(tr_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->tree->root == NULL)
		itor->node = NULL;
	else
		itor->node = node_min(itor->tree->root);
	RETVALID(itor);
}

int
tr_itor_last(tr_itor *itor)
{
	ASSERT(itor != NULL);

	if (itor->tree->root == NULL)
		itor->node = NULL;
	else
		itor->node = node_max(itor->tree->root);
	RETVALID(itor);
}

int
tr_itor_search(tr_itor *itor, const void *key)
{
	int rv;
	tr_node *node;
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
tr_itor_key(const tr_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->key : NULL;
}

void *
tr_itor_data(tr_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->dat : NULL;
}

const void *
tr_itor_cdata(const tr_itor *itor)
{
	ASSERT(itor != NULL);

	return itor->node ? itor->node->dat : NULL;
}

int
tr_itor_set_data(tr_itor *itor, void *dat, int del)
{
	ASSERT(itor != NULL);

	if (itor->node == NULL)
		return -1;

	if (del && itor->tree->dat_del)
		itor->tree->dat_del(itor->node->dat);
	itor->node->dat = dat;
	return 0;
}
