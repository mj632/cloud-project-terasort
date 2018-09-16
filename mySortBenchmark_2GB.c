#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/time.h>

#define TOTAL_CHAR 100		
#define FILE_SIZE 20000000		//--------2GB size
#define LINES 	  5000000		//-------FILE_SIZE/NO_OF_FILES
#define CMP_SIZE 10			//------- used for comparing data size(first 10 chars in the each line)
#define NO_OF_FILES 4
#define THREAD_COUNT 4
#define FINAL_BUFF_SIZE 5000000			//--------   based on no_of_files and RAM size 
#define OUT_BUFF 4

pthread_mutex_t pmt = PTHREAD_MUTEX_INITIALIZER;
int chunk_no = 0;

long int partition(char **, long int , long int );
void quickSort(char **, long int , long int );
void *assign_work(void *arg);

int main()
{
	FILE *inputFileptr,*outputFileptr;
	struct timeval process_start_time, process_end_time;
	gettimeofday(&process_start_time, NULL);
	double total_time_taken;
	pthread_t sortTh[THREAD_COUNT];
	char fileName[100], **outputBuff;
	long long int pos = 0;
	
	//---------------Threading here---------------//
	for(int i = 0; i < THREAD_COUNT; i++)
		pthread_create(&sortTh[i], NULL, assign_work, NULL);

	for(int i = 0; i < THREAD_COUNT; i++)
		pthread_join(sortTh[i], NULL);

/*
	if(NO_OF_FILES == 1)
	{
*/		gettimeofday(&process_end_time, NULL);
		total_time_taken = (float) (process_end_time.tv_usec - process_start_time.tv_usec) / 1000000 + (float) (process_end_time.tv_sec - process_start_time.tv_sec);
		printf("Total time taken for files creation : %f\n",total_time_taken);
//	}
/*	
//=========================================================================================================
//							K-way merge starts from here
//=========================================================================================================
*/
	long int b_line_no[NO_OF_FILES] = {0};			//--------String Buffer line number for i-th file  
	int count[NO_OF_FILES] = {0};		//--------------use it in array
	long int buffSize = (long int) FINAL_BUFF_SIZE;		//----all t0,t1,... will be within range of buffSize
	char ***iBuff;
	
	outputBuff = (char **) calloc(OUT_BUFF*buffSize, sizeof(char *));			//-----Assign each buffer same size of memory
	for(long int i = 0; i < (long int)(OUT_BUFF * buffSize); i++)
	{
		outputBuff[i] = (char *) calloc(TOTAL_CHAR, sizeof(char));
	}

	iBuff = (char ***) calloc (NO_OF_FILES, sizeof(char **));
	for(int l = 0; l < NO_OF_FILES; l++)
	{
		iBuff[l] = (char **) calloc((long int)buffSize, sizeof(char *));
		for(long int k = 0; k < (long int) buffSize; k++)
			iBuff[l][k] =  (char *) calloc(TOTAL_CHAR, sizeof(char));
	}


	for(int fi = 0; fi < NO_OF_FILES; fi++)
	{
		sprintf(fileName, "/tmp/mpatelblock%d",(int)fi);
		inputFileptr = fopen(fileName, "r");			
		pos = 0;
		fseek(inputFileptr,(long long int) pos, SEEK_SET);
		for(long int i = 0; i < (long int)buffSize && fread(iBuff[fi][i], TOTAL_CHAR,  1,inputFileptr) != EOF ; i++)
		{
			pos += TOTAL_CHAR;
			fseek(inputFileptr,(long long int) pos, SEEK_SET);
		}
		fclose(inputFileptr);
		count[fi]++;				
	}

	outputFileptr = fopen("/tmp/mpateloutput", "w");
	fclose(outputFileptr);
	long int opos = 0, output_buff_count = 0;
	int min_i = 0;
	
	while(output_buff_count < ((long int)FILE_SIZE/(OUT_BUFF*buffSize)))
	{
		for(int refill_i = 0; refill_i < NO_OF_FILES; refill_i++)
		{
			if((b_line_no[refill_i] >= (long int)buffSize) && count[refill_i] < (LINES/buffSize))		//----2500/500
			{
				sprintf(fileName, "/tmp/mpatelblock%d",(int)refill_i);
				inputFileptr = fopen(fileName, "r");
				pos = (long long int)count[refill_i] * buffSize * TOTAL_CHAR;
				fseek(inputFileptr,(long long int) pos, SEEK_SET);
				for(long int i = 0; i < (long int)buffSize && fread(iBuff[refill_i][i], TOTAL_CHAR,  1,inputFileptr) != EOF ; i++)
				{
					pos += TOTAL_CHAR;
					fseek(inputFileptr,(long long int) pos, SEEK_SET);
				}
				fclose(inputFileptr);
				b_line_no[refill_i] = 0;
				count[refill_i]++;
			}
		}

		if(opos >= (long int)(buffSize*OUT_BUFF))
		{
			opos = 0;
			output_buff_count++;
			outputFileptr = fopen("/tmp/mpateloutput", "a+");
			fseek(outputFileptr, 0, SEEK_END);
					
			for(long int i = 0; i < (long int)(OUT_BUFF*buffSize) ; i++)
			{
				fwrite(outputBuff[i], TOTAL_CHAR, 1, outputFileptr);		
			}
			fclose(outputFileptr);

//			printf("====================================\n\n\n");
//			printf("output_buff filled for : %ld times\n",output_buff_count);
/*			printf("block%d pointer at: %ld\trefill count: %d\n",0,b_line_no[0],count[0]);
			printf("block%d pointer at: %ld\trefill count: %d\n",1,b_line_no[1],count[1]);
			printf("block%d pointer at: %ld\trefill count: %d\n",2,b_line_no[2],count[2]);
			printf("block%d pointer at: %ld\trefill count: %d\n",3,b_line_no[3],count[3]);
			printf("block%d pointer at: %ld\trefill count: %d\n",4,b_line_no[4],count[4]);
			printf("block%d pointer at: %ld\trefill count: %d\n",5,b_line_no[5],count[5]);
			printf("block%d pointer at: %ld\trefill count: %d\n",6,b_line_no[6],count[6]);
			printf("block%d pointer at: %ld\trefill count: %d\n",7,b_line_no[7],count[7]);
			printf("====================================\n");
			printf("total_size_of_output_file : %ld\n",total_size_of_output_file);
			printf("LAST LINE FROM THE block:\n%s",outputBuff[buffSize-1]);
			printf("====================================\n");
*/			

			if(output_buff_count >= ((long int) FILE_SIZE/(buffSize*OUT_BUFF)))
			{	
				break;
			}
		}
		//------find something for min_i
		for(min_i = 0; min_i < NO_OF_FILES; min_i++)
		{
			if(count[min_i] <= ((LINES/buffSize) + 1) && b_line_no[min_i] < buffSize)
				break;
		}
		for(int block_i = min_i+1; block_i < NO_OF_FILES ; block_i++)
		{
			if(count[block_i] < ((LINES/buffSize) + 1) && b_line_no[block_i] < buffSize)
				if(strncmp(iBuff[min_i][b_line_no[min_i]], iBuff[block_i][b_line_no[block_i]], CMP_SIZE) > 0)
				{
					min_i = block_i;
				}
		}
		if(min_i < NO_OF_FILES)
		{
			strncpy(outputBuff[opos++], iBuff[min_i][b_line_no[min_i]], TOTAL_CHAR);
			b_line_no[min_i]++;
		}
	}

	
	gettimeofday(&process_end_time, NULL);
	total_time_taken = (float) (process_end_time.tv_usec - process_start_time.tv_usec) / 1000000 + (float) (process_end_time.tv_sec - process_start_time.tv_sec);
	printf("Total time taken : %f\n",total_time_taken);
	free(iBuff);
	return 0;
}

void *assign_work(void *arg)
{
	int block_no;
		pthread_mutex_lock(&pmt);
			block_no = chunk_no++;
		pthread_mutex_unlock(&pmt);

	FILE *inputFileptr,*outputFileptr;
	long long int pos;
	char **inputBuff,fileName[100];
	size_t len;
	ssize_t read_char;

	inputBuff = (char **) calloc((long int)LINES, sizeof(char *));
	while(block_no < NO_OF_FILES)
	{
		inputFileptr = fopen("/input/data-2GB.in", "r");
		pos = (long long int)block_no * TOTAL_CHAR * LINES;
		fseek(inputFileptr,(long long int) pos, SEEK_SET);
		for(long int i = 0; i < (long int)LINES && (read_char = getline(&inputBuff[i], &len, inputFileptr)) != -1; i++);
		fclose(inputFileptr);
//		quickSort(inputBuff, start_point, end_point);
		quickSort(inputBuff, 0, LINES-1);

		//===============NEXT TASK IS TO MERGE ALL THE THREADS 
		sprintf(fileName, "/tmp/mpatelblock%d",block_no);
		outputFileptr = fopen(fileName, "w+");
		fseek(outputFileptr, 0, SEEK_END);

		for(long int i = 0; i < (long int)LINES ; i++)
		{
			fwrite(inputBuff[i], strlen(inputBuff[i]), 1, outputFileptr);		
		}
		fclose(outputFileptr);				//===============DO NOT FORGET TO CLOSE THE FILE
		pthread_mutex_lock(&pmt);
			block_no = chunk_no++;
		pthread_mutex_unlock(&pmt);
	}	
	free(inputBuff);
	pthread_exit(NULL);
	return NULL;
}


long int partition(char **unsorted_list, long int start_point, long int end_point)
{
	char *swapper;
	long int i = start_point - 1;
	swapper = malloc ( TOTAL_CHAR * sizeof(char));
	memset(swapper, 0, TOTAL_CHAR);
	
	for(long int j = start_point; j < end_point; j++)
	{
		if(strncmp(unsorted_list[j], unsorted_list[end_point], CMP_SIZE) <= 0)
		{
			i++;											//------Swapping the data 
			strncpy(swapper, unsorted_list[i], TOTAL_CHAR);
			strncpy(unsorted_list[i], unsorted_list[j], TOTAL_CHAR);
			strncpy(unsorted_list[j], swapper, TOTAL_CHAR);
		}
	}

	i++;
	strncpy(swapper, unsorted_list[i], TOTAL_CHAR);
	strncpy(unsorted_list[i], unsorted_list[end_point], TOTAL_CHAR);
	strncpy(unsorted_list[end_point], swapper, TOTAL_CHAR);
	free(swapper);
	return i;		//pivot pointer position
}

void quickSort(char **unsorted_list, long int start_point, long int end_point)
{
	if(start_point < end_point)
 	{
		int pivot = partition(unsorted_list, start_point, end_point);
		
		quickSort(unsorted_list, start_point, pivot - 1);
		quickSort(unsorted_list, pivot + 1, end_point);
	}
}
