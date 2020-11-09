#include <stdlib.h>
#include <stdio.h>

#include "bst.h"

// Local functions
static int place_bst_node( struct BST_Node *parent, struct BST_Node *node );
static struct BST_Node *make_bst_node( int key, void *data );

// Root's pointer is passed because root can get modified for the first node
int bst_add_node( struct BST_Node **root, int key, void *data )
{
	struct BST_Node *newnode = NULL;
	struct BST_Node *parent = NULL;
	struct BST_Node *retnode = NULL;
	int status = 0;

	newnode = make_bst_node( key, data);
	if( *root == NULL ){
		*root = newnode;
		status = BST_SUCCESS;
	}
	else{
		status = place_bst_node( *root, newnode );
	}
	return status;
}

int flag;
//Code for struct of BST_Delete referenced from: https://www.geeksforgeeks.org/binary-search-tree-set-2-delete/
struct BST_Node* deleteNode(struct BST_Node *root,int key){
	// base case 
	if(root==NULL){
		return NULL;
	}
	// If the key to be deleted is smaller than the root's key, 
    // then it lies in left subtree 
	if(root->key>key){
		root->left_child=deleteNode(root->left_child,key);
		return root;
	}
	// If the key to be deleted is greater than the root's key, 
    // then it lies in right subtree 
	else if(root->key<key){
		root->right_child=deleteNode(root->right_child,key);
		return root;
	}
	// if key is same as root's key, then This is the node 
    // to be deleted 
	flag=1;
	if(root->left_child==NULL || root->right_child==NULL){
		if(root->right_child!=NULL){
			struct BST_Node* node=root->right_child;
			free(root);
			return node;
		}
		else if(root->left_child!=NULL){
			struct BST_Node* node=root->left_child;
			free(root);
			return node;
		}
		free(root);
		return NULL;
	}
	struct BST_Node* temp_node_1=root->right_child;
	struct BST_Node* temp_node_2=root;
	while(temp_node_1->left_child!=NULL){
		temp_node_2=temp_node_1;
		temp_node_1=temp_node_1->left_child;
	}
	if(temp_node_1==root->right_child){
		temp_node_1->left_child=root->left_child;
		free(root);
		return temp_node_1;
	}
	temp_node_2->left_child=temp_node_1->right_child;
	temp_node_1->left_child=root->left_child;
	temp_node_1->right_child=root->right_child;
	free(root);
	return temp_node_1;
}
//referenced code End

int bst_del_node( struct BST_Node **root, int key ){
	flag=0;
	*root=deleteNode(*root,key);
	if(flag==1)return BST_SUCCESS;
	return BST_NULL;
}

struct BST_Node *bst_search( struct BST_Node *root, int key )
{
	struct BST_Node *retval = NULL;

	if( root == NULL ){
		return NULL;
	}
	else if( root->key == key )
		return root;
	else if( key < root->key )
		return bst_search( root->left_child, key );
	else if( key > root->key )
		return bst_search( root->right_child, key );
}
void bst_print( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		printf("%d ", root->key);
		bst_print( root->left_child );
		bst_print( root->right_child );
	}
}

void bst_free( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root);
	}
}

void bst_destroy( struct BST_Node *root )
{
	if( root == NULL )
		return;
	else{
		bst_free( root->left_child );
		bst_free( root->right_child );
		free(root->data);
		free(root);
	}
}

static int place_bst_node( struct BST_Node *parent, struct BST_Node *node )
{
	int retstatus;

	if( parent == NULL ){
		return BST_NULL;
	}
	else if( node->key == parent->key ){
		return BST_DUP_KEY;
	}
	else if( node->key < parent->key ){
		if( parent->left_child == NULL ){
			parent->left_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->left_child, node );
		}
	}
	else if( node->key > parent->key ){
		if( parent->right_child == NULL ){
			parent->right_child = node;
			return BST_SUCCESS;
		}
		else{
			return place_bst_node( parent->right_child, node );
		}
	}
}

static struct BST_Node *make_bst_node( int key, void *data )
{
	struct BST_Node *newnode;
	newnode = (struct BST_Node *) malloc(sizeof(struct BST_Node));
	newnode->key = key;
	newnode->data = data;
	newnode->left_child = NULL;
	newnode->right_child = NULL;

	return newnode;
}

