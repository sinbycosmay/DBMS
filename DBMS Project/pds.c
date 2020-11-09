#include<stdio.h>
#include<string.h>
#include<errno.h>
#include<stdlib.h>

#include"bst.h"
#include"pds.h"

//struct PDS_RepoInfo repo_handle;
struct PDS_DB_Master db_handle;
struct PDS_RepoInfo *extract_name( char * entity_name ){
    int num_entities = db_handle.db_info.num_entities;
    for( int i=0; i<num_entities; i++){
        if( strcmp( db_handle.entity_info[i].entity_name, entity_name ) == 0 ){
            return &db_handle.entity_info[i];
        }
    }
}
struct PDS_LinkFileInfo *extract_link(char *name){
    for (int i = 0; i < db_handle.db_info.num_relationships; i++){
        char tmp[100];
        strcpy(tmp, db_handle.db_info.links[i].pds_name1);
        strcat(tmp, "_");
        strcat(tmp, db_handle.db_info.links[i].pds_name2);
        if (strcmp(tmp, name) == 0){
            return &db_handle.rel_info[i];
        }
    }
}

int link_data(char *link_name, int key, int linked_key){
    int status;
    for(int i = 0; i < db_handle.db_info.num_relationships ; i++){
        char tmp[100];
        strcpy(tmp, db_handle.db_info.links[i].pds_name1);
        strcat(tmp, "_");
        strcat(tmp, db_handle.db_info.links[i].pds_name2);
        if (strcmp(tmp, link_name) == 0){ 
            int flag = 0;
            struct PDS_RepoInfo* repo_handle2 = extract_name(db_handle.db_info.links[i].pds_name2);
            struct PDS_RepoInfo* repo_handle1 = extract_name(db_handle.db_info.links[i].pds_name1);
            struct BST_Node *left = bst_search(repo_handle1->pds_bst, key);
            struct BST_Node *right = bst_search(repo_handle2->pds_bst, linked_key);            
            struct PDS_LinkedKeySet *linked_data = (struct PDS_LinkedKeySet *)malloc(sizeof(struct PDS_LinkedKeySet));
            get_linked_data(link_name, key, linked_data);
            for(int ix = 0; ix < linked_data->link_count; ix++){
                if(linked_data->linked_keys[ix] == linked_key){
                    flag = 1;
                    }
            }
            if(left == NULL || right == NULL || flag == 1){
                return PDS_LINK_FAILED;
                break;
                }
        }
        else{
            status = PDS_LINK_FAILED;
            return PDS_LINK_FAILED;
        }
    }
    return PDS_SUCCESS;
}

int get_linked_data( char *link_name, int data_key, struct PDS_LinkedKeySet *linked_data ){
    int status;
    struct PDS_LinkFileInfo *lnk;
    lnk = extract_link(link_name);
    if (lnk == NULL){
        status = PDS_LINK_FAILED;
        }
    else{
        struct PDS_Link link;
        fseek(lnk->pds_link_fp, 0, SEEK_SET);  
        linked_data->link_count = 0;
        linked_data->key = data_key;
        while (!feof(lnk->pds_link_fp)){
            int flag = fread(&link, sizeof(struct PDS_Link), 1, lnk->pds_link_fp);
            if (flag == 0){
                break;
                }
            else if (link.key == data_key){
                linked_data->linked_keys[linked_data->link_count] = link.linked_key;
                linked_data->link_count++;
            }
        }
        status = PDS_SUCCESS;
    }
    return status;
}

int pds_db_open( char *db_name ){
    char db_file_name[50];
    int status;
//printf("&& %d",db_handle.db_status);
    int num_entities=0, num_relations=0;
    if( db_handle.db_status == PDS_DB_OPEN ) return PDS_DB_ALREADY_OPEN;
    db_handle.db_status = PDS_DB_OPEN;
    strcpy( db_file_name, db_name);
    strcat( db_file_name, ".db");
    FILE* fptr = (FILE *) fopen( db_file_name, "rb+");
    fread( &db_handle.db_info, sizeof( struct PDS_DBInfo ), 1, fptr);
    num_entities = db_handle.db_info.num_entities;
    num_relations = db_handle.db_info.num_relationships;
    for( int i=0; i<num_entities; i++){
strcpy(db_handle.entity_info[i].entity_name,db_handle.db_info.entities[i].entity_name);
		db_handle.entity_info[i].entity_size=db_handle.db_info.entities[i].entity_size;
pds_open(db_handle.entity_info[i].entity_name,db_handle.entity_info[i].entity_size);
    }
    for(int i=0;i<num_relations;i++){
        char tmp[30];
        strcpy(db_handle.rel_info[i].link_name, db_handle.db_info.links[i].link_name);
        strcpy(tmp, db_handle.db_info.links[i].pds_name1);
        strcat(tmp, "_");
        strcat(tmp, db_handle.db_info.links[i].pds_name2);
        strcat(tmp, ".lnk");
        db_handle.rel_info[i].pds_link_fp = fopen(tmp, "rb+");
        if (db_handle.rel_info[i].pds_link_fp == NULL){
            return PDS_FILE_ERROR;
            }
        db_handle.rel_info[i].link_status = PDS_ENTITY_OPEN;
        for (int ix = 0; ix < MAX_FREE; ix++){
            db_handle.rel_info[i].free_list[ix] = -1;
            }
    }
//printf("&$$$& %d",db_handle.db_status);
    return PDS_SUCCESS;
}


// int pds_db_open(char *db_name ){
//     char repo_file[30];
//     strcpy(repo_file,db_name);
//     strcat(repo_file,".db");
//     int status;
//     FILE* db_fp = (FILE *)fopen(repo_file,"rb+");
//     while(fread(db_handle.db_info, sizeof(db_handle),1,db_fp)){
//         for(int i =0;i<db_handle.db_info.num_entities;i++){
//             strcpy(db_handle.entity_info[i].entity_name,db_handle.db_info.entities[i].entity_name);
//             db_handle.entity_info[i].entity_size = db_handle.db_info.entities[i].entity_size;
//         }
//         for(int i=0;i<db_handle.db_info.num_relationships;i++){
//             strcpy(db_handle.rel_info[i].link_name,db_handle.db_info.links[i].link_name);
//             db_handle.rel_info[i].link_status = PDS_DB_CLOSED;
//             char lfp[50];
//             strcpy(lfp, db_handle.db_info.links[i].link_name);
//             strcat(lfp,".lnk");
//             db_handle.rel_info[i].pds_link_fp = fopen(lfp,"rb+");
//             db_handle.rel_info[i].link_status = PDS_ENTITY_OPEN;
//         }
//         db_handle.db_status = PDS_DB_OPEN;
//         fclose(db_fp);
//         return status;
//     }

// }

int pds_open(char *repo_name, int entity_size){
	struct PDS_RepoInfo *repo_handle = extract_name(repo_name);
    //if(repo_handle->repo_status==PDS_ENTITY_OPEN)return PDS_ENTITY_ALREADY_OPEN;
    char repo_file[30];
    char ndx_file[30];
    if (repo_handle->repo_status == PDS_ENTITY_OPEN){
        return PDS_ENTITY_ALREADY_OPEN;
    }

    strcpy(repo_handle->entity_name, repo_name);

    strcpy(repo_file,repo_name);
    strcat(repo_file,".dat");
    strcpy(ndx_file,repo_name);
    strcat(ndx_file,".ndx");
    int status;

    repo_handle->pds_data_fp = (FILE *)fopen(repo_file,"rb+");
    if(repo_handle->pds_data_fp == NULL){
        perror(repo_file);
    }

    repo_handle->pds_ndx_fp = (FILE *)fopen(ndx_file,"rb+");
    if(repo_handle->pds_data_fp == NULL){
        perror(ndx_file);
    }

    repo_handle->repo_status = PDS_ENTITY_OPEN;
    repo_handle->entity_size =  entity_size;
    repo_handle->pds_bst = NULL;

    status = pds_load_ndx(repo_handle);
    return status;

}

//Code for isEmpty Referenced from:
//https://stackoverflow.com/questions/30133210/check-if-file-is-empty-or-not-in-c?noredirect=1&lq=1
int isEmpty(FILE *file){
    long savedOffset = ftell(file);
    fseek(file, 0, SEEK_END);

    if (ftell(file) == 0){
        return 1;
    }

    fseek(file, savedOffset, SEEK_SET);
    return 0;
}

//referenced code end :D

int pds_load_ndx(struct PDS_RepoInfo *repo_handle){
    int status;
        if(isEmpty(repo_handle->pds_ndx_fp)){
            memset(repo_handle->free_list,-1,sizeof(repo_handle->free_list));
        }
        else{
        fread(repo_handle->free_list,sizeof(int),100,repo_handle->pds_ndx_fp);
        }
    while (!feof(repo_handle->pds_ndx_fp))
    {
        struct PDS_NdxInfo *ndx_entry;
        ndx_entry = (struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
        int is_there;
        is_there = fread(ndx_entry,sizeof(struct PDS_NdxInfo), 1, repo_handle->pds_ndx_fp);
        if(is_there==0)break;
        status = bst_add_node(&repo_handle->pds_bst, ndx_entry->key, ndx_entry);
        if(status!=BST_SUCCESS)
        {
            return PDS_NDX_SAVE_FAILED;
        }
    }
    return PDS_SUCCESS;

}



int put_rec_by_key(char *entity_name, int key, void *rec){
    int offset, status, writesize;
    struct PDS_NdxInfo *ndx_entry;
    struct PDS_RepoInfo* repo_handle =  extract_name( entity_name );
    fseek(repo_handle->pds_data_fp,0,SEEK_END);

    offset = ftell(repo_handle->pds_data_fp);
    int flag=-1;
    for(int i=0;i<100;i++){
        if(repo_handle->free_list[i]>=0){
            flag=i;
            offset=repo_handle->free_list[i];
            break;
        }
    }
    ndx_entry = (struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));

    ndx_entry->key = key;
    ndx_entry->offset = offset;
    status = bst_add_node(&repo_handle->pds_bst, ndx_entry->key, ndx_entry);

    if(status != BST_SUCCESS){
        free(ndx_entry);
        fseek(repo_handle->pds_data_fp, offset, SEEK_SET);
        status = PDS_ADD_FAILED;
    }
    
    {

        if(flag>=0){
            repo_handle->free_list[flag]=-1;
        }
		fseek(repo_handle->pds_data_fp, offset, SEEK_SET);
            fwrite(&key, sizeof(int), 1, repo_handle->pds_data_fp);
        writesize = fwrite(rec,repo_handle->entity_size,1,repo_handle->pds_data_fp);
        status = PDS_SUCCESS;
    }

    return status;
    
}
/*
int put_rec_by_key(char *entity_name,int key, void *rec){

	struct PDS_RepoInfo* repo_handle =  extract_name(entity_name);
	int offset=-1,offset1,pos=-1, status, writesize;
	struct PDS_NdxInfo *ndx_entry;
	struct BST_Node index_node;
	if (repo_handle->repo_status == PDS_ENTITY_CLOSED){
		return PDS_ENTITY_NOT_OPEN;
	}
	for(int i=0;i<MAX_FREE;i++){
		if(repo_handle->free_list[i]!=-1){
			offset = repo_handle->free_list[i];
			pos=i;
			break;
		}
	}
	if(offset==-1){
		fseek(repo_handle->pds_data_fp, 0, SEEK_END);
		offset = ftell(repo_handle->pds_data_fp);
		}
	
	ndx_entry = (struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
	ndx_entry->key = key;
	ndx_entry->offset = offset;
	status = bst_add_node(&(repo_handle->pds_bst), key, ndx_entry);
	if(status==BST_DUP_KEY){
		free(ndx_entry);
	}
	else if(status!=BST_SUCCESS){
		fprintf(stderr, "Unable to add index entry for key %d - Error %d\n", key, status);
		free(ndx_entry);
		fseek(repo_handle->pds_data_fp, 0, SEEK_END);
		status = PDS_ADD_FAILED;
	}
	else{
		fseek(repo_handle->pds_data_fp, offset, SEEK_SET);
		fwrite(&key, sizeof(int), 1, repo_handle->pds_data_fp);
		writesize = fwrite(rec, repo_handle->entity_size, 1, repo_handle->pds_data_fp);
		status = PDS_SUCCESS;
		if(pos!=-1)
			repo_handle->free_list[pos]=-1;
	}
	return status;
}
*/
int get_rec_by_ndx_key(char *entity_name, int key, void *rec){
    struct PDS_NdxInfo *ndx_entry;
    struct BST_Node *bst_node;
    int offset,status,readsize;
    struct PDS_RepoInfo* repo_handle =  extract_name( entity_name );
bst_print(repo_handle->pds_bst);
    bst_node = bst_search(repo_handle->pds_bst,key);
    if(bst_node == NULL){
        status = PDS_REC_NOT_FOUND;
    }
    else
    {
        ndx_entry=(struct PDS_NdxInfo*) bst_node->data;
        offset = ndx_entry->offset;
        fseek(repo_handle->pds_data_fp, offset, SEEK_SET);
        int temp;
        fread(&temp, sizeof(int), 1, repo_handle->pds_data_fp);
        fread(rec, repo_handle->entity_size, 1, repo_handle->pds_data_fp);
        status = PDS_SUCCESS;
    }

    return status;
    
}

int delete_by_key(char *entity_name,int key){
    struct PDS_RepoInfo* repo_handle =  extract_name( entity_name );
    if(repo_handle->repo_status == PDS_ENTITY_OPEN){
        int nor = db_handle.db_info.num_relationships;
        for(int i =0;i<nor;i++){
        struct PDS_LinkedKeySet *link_dat = (struct PDS_LinkedKeySet *)malloc(sizeof(struct PDS_LinkedKeySet));
        if (strcmp(db_handle.db_info.links[i].pds_name2, entity_name) == 0){
            struct PDS_Link lnk;
            fseek(db_handle.rel_info[i].pds_link_fp, 0, SEEK_SET);
            while (!feof(db_handle.rel_info[i].pds_link_fp))
            {
                int flag_1 = fread(&lnk, sizeof(struct PDS_Link), 1, db_handle.rel_info[i].pds_link_fp);
                if (lnk.linked_key == key){
                    return PDS_DELETE_FAILED;
                    }
                if (flag_1 == 0){
                    break;
                    }
                
            }
        }
        else if (strcmp(db_handle.db_info.links[i].pds_name1, entity_name) == 0){
            char tmp[100];
            strcpy(tmp, entity_name);
            strcat(tmp, "_");
            strcat(tmp, db_handle.db_info.links[i].pds_name2);
            get_linked_data(tmp, key, link_dat);
            if (link_dat->link_count > 0){
                return PDS_DELETE_FAILED;
                }
        }
        free(link_dat);
        }
        struct BST_Node *index;
        index = bst_search( repo_handle->pds_bst, key );
        if(index == NULL){
            return PDS_REC_NOT_FOUND;
        }
        else{
        struct PDS_NdxInfo *temp;
        temp = index->data;
        int temp_offset = temp->offset;
        int breaking_flag=0;
        for(int i=0;i<MAX_FREE;i++){
            if(repo_handle->free_list[i]==-1){
                repo_handle->free_list[i]=temp_offset;
                breaking_flag=1;
                break;
            }
        }
        if(breaking_flag==1){
bst_del_node(&repo_handle->pds_bst, key);
            return PDS_SUCCESS;
        }
        return PDS_DELETE_FAILED;
        }
    }
    else
    {
        return PDS_FILE_ERROR;
    }
    
}


int get_rec_by_non_ndx_key(char *entity_name, void *key, void *rec, int (*matcher)(void *rec, void *key),int* io_count){
    int status;
    int count=0,count1 = 0;
	struct PDS_RepoInfo *repo_handle = extract_name(entity_name);
    fseek(repo_handle->pds_data_fp, 0, SEEK_SET);
    while(!feof(repo_handle->pds_data_fp)){
        int flag, key1;
        count+=1;
        int offset=ftell(repo_handle->pds_data_fp);
        flag = fread(&key1, sizeof(int), 1, repo_handle->pds_data_fp);
        if(flag == 0)
        {
            status = PDS_REC_NOT_FOUND;
            break;
        }
        fread(rec, repo_handle->entity_size, 1, repo_handle->pds_data_fp);
        int checker = matcher(rec, key);
        if(checker > 1)
        {
            status = PDS_REC_NOT_FOUND;
            break;
        }
        for(int i=0;i<MAX_FREE;i++){
            if(repo_handle->free_list[i] == offset){
                count1++;
                checker = 1;
                break;
            }
        }
        if(checker == 0){
            status = PDS_SUCCESS;
            break;
        }
    }
    *io_count = count - count1;
    return status;
}

int update_by_key(char *entity_name, int key, void *newrec){
    struct PDS_RepoInfo* repo_handle =  extract_name( entity_name );
    if(repo_handle->repo_status == PDS_ENTITY_OPEN){
        struct BST_Node *index;
        index = bst_search(repo_handle->pds_bst, key);
        if(index == NULL){
            return PDS_MODIFY_FAILED;
        }
        else{
            struct PDS_NdxInfo *temp;
            temp = index->data;
            int free_offset = temp->offset;
            fseek(repo_handle->pds_data_fp, free_offset, SEEK_SET);
            fwrite(&key, sizeof(int), 1, repo_handle->pds_data_fp);
            fwrite(newrec, repo_handle->entity_size, 1, repo_handle->pds_data_fp);
        }
        return PDS_SUCCESS;

    }
    else{
        return PDS_FILE_ERROR;
    }
}

int preorder(char *entity_name,struct BST_Node* node) 
{ 
	struct PDS_RepoInfo *repo_handle = extract_name(entity_name);
    struct PDS_NdxInfo *ndx_entry;
    int writesize, status;
    if (node == NULL) return PDS_SUCCESS; 
    ndx_entry = (struct PDS_NdxInfo *)malloc(sizeof(struct PDS_NdxInfo));
    ndx_entry = (struct PDS_NdxInfo *)node->data;
    writesize = fwrite(ndx_entry, sizeof(struct PDS_NdxInfo), 1, repo_handle->pds_ndx_fp);
    status = preorder(entity_name,node->left_child);
    if(status!=PDS_SUCCESS)return status;  
    status = preorder(entity_name,node->right_child);
    if(status!=PDS_SUCCESS)return status;
    return PDS_SUCCESS; 
}
int pds_db_close(){
	int status;
	for (int i=0;i<db_handle.db_info.num_entities;i++){
		status = pds_close(db_handle.entity_info[i].entity_name);
		if(status!=PDS_SUCCESS){
			break;
		}
	}
    for (int i = 0; i < db_handle.db_info.num_relationships; i++){
        fclose(db_handle.rel_info[i].pds_link_fp);
        db_handle.rel_info[i].link_status = PDS_ENTITY_CLOSED;
        strcpy(db_handle.rel_info[i].link_name, "");
    }
    db_handle.db_status = PDS_DB_CLOSED;
	return status;
}

int pds_create_schema (char *schema_file_name){
    struct PDS_DBInfo info;
    FILE* temp = (FILE *) fopen(schema_file_name, "r");
    fscanf(temp, "%s", info.db_name);
    strcat(info.db_name, ".db");
    FILE* db  = (FILE *) fopen(info.db_name, "wb+");
    info.num_entities = 0;
    info.num_relationships = 0;
    int status;
    while(!feof(temp)){
        char temp_arr[30];
        int flag = fscanf(temp, "%s", temp_arr);
        if(flag == 0){
            break;
            }
        if(strcmp(temp_arr, "relationship") == 0){
            fscanf(temp, "%s", info.links[info.num_relationships].link_name);
            fscanf(temp, "%s", info.links[info.num_relationships].pds_name1);
            fscanf(temp, "%s", info.links[info.num_relationships].pds_name2);
            info.num_relationships+=1;
        }    
        else if(strcmp(temp_arr, "entity") == 0){
            fscanf(temp, "%s", info.entities[info.num_entities].entity_name);
            fscanf(temp, "%d", &info.entities[info.num_entities].entity_size);
            info.num_entities+=1;
        }
        else{
            status = PDS_FILE_ERROR;
            return status;
            }
    }
    status = PDS_SUCCESS;
    fwrite(&info, sizeof(info), 1, db);
    fclose(db);
    return status;
}

int pds_close(char *entity_name){
    struct PDS_RepoInfo* repo_handle =  extract_name( entity_name );
    if (repo_handle->repo_status == PDS_ENTITY_CLOSED)return PDS_ENTITY_NOT_OPEN;
    char ndx_file[30];
    strcpy(ndx_file,repo_handle->entity_name);
    strcat(ndx_file,".ndx");
    repo_handle->pds_ndx_fp = (FILE *)fopen(ndx_file,"wb");
    fwrite(repo_handle->free_list,sizeof(int),100,repo_handle->pds_ndx_fp);
    int status = preorder(entity_name,repo_handle->pds_bst);
    fclose(repo_handle->pds_data_fp);
    fclose(repo_handle->pds_ndx_fp);
    strcpy(repo_handle->entity_name, "");
    bst_destroy(repo_handle->pds_bst);
    repo_handle->repo_status = PDS_ENTITY_CLOSED;
    return status;
}
