#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include "pds.h"
#include "academia.h"

#define TREPORT(a1,a2) printf("Status %s: %s\n\n",a1,a2); fflush(stdout);

void process_line( char *test_case );

int main(int argc, char *argv[])
{
	FILE *cfptr;
	char test_case[50];

	if( argc != 2 ){
		fprintf(stderr, "Usage: %s testcasefile\n", argv[0]);
		exit(1);
	}

	cfptr = (FILE *) fopen(argv[1], "r");
	int counter=0;
	while(fgets(test_case, sizeof(test_case)-1, cfptr)){
		// printf("line:%s",test_case);
		if( !strcmp(test_case,"\n") || !strcmp(test_case,"") )
			continue;
		printf("Test case %d: %s", ++counter, test_case); fflush(stdout);
		process_line( test_case );
	}
	return PDS_SUCCESS;
}

void process_line( char *test_case )
{
	char db_name[30], entity_name[30];
	char command[50], param1[50], param2[50], param3[50], info[1000];
	char name[10];
	int rollnumber, entity_id, status, student_size, course_size, expected_status, expected_io, actual_io;
	char expected_name[50];
	char expected_address[50];
	struct Student testStudent;
	struct Course testCourse;

	student_size = sizeof(struct Student);
	course_size = sizeof(struct Course);

	sscanf(test_case, "%s%s%s%s", command, param1, param2, param3);
	if( !strcmp(command,"CREATEDB") ){
		strcpy(db_name, param1);
		if( !strcmp(param2,"0") )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;
		status = pds_create_schema( db_name );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status == expected_status ){
			TREPORT("PASS", "");
		}
		else{
			sprintf(info,"pds_schema_create returned status %d",status);
			TREPORT("FAIL", info);
		}

	}
	else if( !strcmp(command,"DBOPEN") ){
		strcpy(db_name, param1);
		if( !strcmp(param2,"0") )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;
		status = pds_db_open( db_name );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status == expected_status ){
			TREPORT("PASS", "");
		}
		else{
			sprintf(info,"pds_db_open returned status %d",status);
			TREPORT("FAIL", info);
		}

	}
	else if( !strcmp(command,"STORE") ){
		strcpy(entity_name, param1);
		if( !strcmp(param3,"0") )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;

		sscanf(param2, "%d", &entity_id);
		testStudent.rollnumber = entity_id;
		sprintf(testStudent.student_name,"Name-of-%d",entity_id);
		sprintf(testStudent.date_of_birth,"DOB-of-%d",entity_id);
		sprintf(testStudent.address,"Address-of-%d",entity_id);
		status = put_rec_by_key( entity_name, entity_id, &testStudent );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status == expected_status ){
			TREPORT("PASS", "");
		}
		else{
			sprintf(info,"put_rec_by_key returned status %d",status);
			TREPORT("FAIL", info);
		}
	}
	else if( !strcmp(command,"KEY_MODIFY") ){
		strcpy(entity_name, param1);
		if( !strcmp(param3,"0") )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;

		sscanf(param2, "%d", &rollnumber);
		testStudent.rollnumber = rollnumber;
		sprintf(testStudent.student_name,"Modified-Name-of-%d",rollnumber);
		sprintf(testStudent.address,"Modified-Address-of-%d",rollnumber);
		status = update_by_key( entity_name, rollnumber, &testStudent );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status == expected_status ){
			TREPORT("PASS", "");
		}
		else{
			sprintf(info,"update_by_key returned status %d",status);
			TREPORT("FAIL", info);
		}
	}
	else if( !strcmp(command,"NDX_SEARCH") ){
		strcpy(entity_name, param1);
		if( strcmp(param3,"0") == 0 )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;

		sscanf(param2, "%d", &rollnumber);
		testStudent.rollnumber = -1;
		status = get_rec_by_ndx_key( entity_name, rollnumber, &testStudent );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status != expected_status ){
			sprintf(info,"search key: %d; Got status %d",rollnumber, status);
			TREPORT("FAIL", info);
		}
		else{
			// Check if the retrieved values match
			if( expected_status == 0 ){
				sprintf(expected_name,"Name-of-%d",rollnumber);
				sprintf(expected_address,"Address-of-%d",rollnumber);
				if (testStudent.rollnumber == rollnumber && 
					strstr(testStudent.student_name,expected_name) != NULL &&
					strstr(testStudent.address,expected_address) != NULL){
						TREPORT("PASS", "");
				}
				else{
					sprintf(info,"Student data not matching... Expected:{%d,%s,%s} Got:{%d,%s,%s}\n",
						rollnumber, expected_name, expected_address, 
						testStudent.rollnumber, testStudent.student_name, testStudent.address
					);
					TREPORT("FAIL", info);
				}
			}
			else
				TREPORT("PASS", "");
		}
	}
	else if( !strcmp(command,"KEY_DELETE") ){
		strcpy(entity_name, param1);
		if( strcmp(param3,"0") == 0 )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;

		sscanf(param2, "%d", &rollnumber);
		testStudent.rollnumber = -1;
		status = delete_by_key( entity_name, rollnumber );
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status != expected_status ){
			sprintf(info,"delete key: %d; Got status %d",rollnumber, status);
			TREPORT("FAIL", info);
		}
		else{
			TREPORT("PASS", "");
		}
	}
	else if( !strcmp(command,"DBCLOSE") ){
		if( strcmp(param1,"0") == 0 )
			expected_status = ACADEMIA_SUCCESS;
		else
			expected_status = ACADEMIA_FAILURE;

		status = pds_db_close();
		if(status == PDS_SUCCESS)
			status = ACADEMIA_SUCCESS;
		else
			status = ACADEMIA_FAILURE;
		if( status == expected_status ){
			TREPORT("PASS", "");
		}
		else{
			sprintf(info,"pds_close returned status %d",status);
			TREPORT("FAIL", info);
		}
	}
	else if( !strcmp(command,"STORE_LINK") ){
		// TO-DO
		status = ACADEMIA_SUCCESS;
	}
	else if( !strcmp(command,"GET_LINKS") ){
		// TO-DO
		status = ACADEMIA_SUCCESS;
	}
}

