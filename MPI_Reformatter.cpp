#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <math.h>
#include <mpi.h>
#include <stdbool.h>
#include <unistd.h>
#include <limits.h>
#include <iostream>


using namespace std;

char *fileName;
const char* genre[] = {"horror\n", "comedy\n", "fantasy\n", "science-fiction\n"};
int paragraphs = 0;
int doneReading = 0;

char *strrev(char *str)
{
      char *p1, *p2;

      if (! str || ! *str)
            return str;
      for (p1 = str, p2 = str + strlen(str) - 1; p2 > p1; ++p1, --p2)
      {
            *p1 ^= *p2;
            *p2 ^= *p1;
            *p1 ^= *p2;
      }
      return str;
}


bool isConsonnant(char ch) {
	if(('B' <= ch && ch <= 'Z') || ('b' <= ch && ch <= 'z')) {
		bool result = (ch != 'E' && ch != 'I' && ch != 'O' && ch != 'U' &&
			ch != 'e' && ch != 'i' && ch != 'o' && ch != 'u');

		return result;
	}

	return false;
}

string horrorFunction(char* paragraph) {
	int size = strlen(paragraph);

	string auxRes = "";

	for(int i = 0; i < 6; i++){
		auxRes += paragraph[i]; // copy horror as-it-is
	}

	for(int i = 6; i < size; i++) {
		if (isConsonnant(paragraph[i])) {
			if(98 <= paragraph[i] && 132 >= paragraph[i]) {
				auxRes += paragraph[i];
				auxRes += paragraph[i];
			}
			else {
				auxRes += paragraph[i];
				auxRes += (paragraph[i] + 32);
			}
		} else {
			auxRes += paragraph[i];
		}
	}

	return auxRes;
}

char* comedyFunction(char* paragraph) {
	char* token = strpbrk(paragraph, " \n");

	while(token != NULL) {
		if(token[1] != '\0') {
			int i = 1;
			while(token[i] != '\0' && token[i] != ' ' && token[i] != '\n'){
				if(i % 2 == 0 &&('a' <= token[i] && token[i] <= 'z')) {
					token[i] -= 32;
				}
				i++;
			}
		}
		token = strpbrk(token + 1, " \n");
	}

	return paragraph;
}

char* fantasyFunction(char* paragraph) {
	char *token = strpbrk(paragraph, " \n");

	while(token != NULL) {
		if(token[1] != '\0') {
			if('a' <= token[1] && token[1] <= 'z') {
				token[1] -= 32;
			}
		}
		token = strpbrk(token + 1, " \n");
	}
	return paragraph;
}

char* sfFunction(char* paragraph) {
	char *token = strpbrk(paragraph, " \n");
	int i = 0;
	while(token != NULL) {
		if(token[0] == '\n' || token[0] == '\n')
			i = 0;
		if(token[1] != '\0' && token[1] != ' ' && token[1] != '\n') { // am gasit un cuvant
			i++;
			//cate al 7-lea element de pe fiecare linie a paragrafului
			if(i == 7) {
			//reverse the word
			int length = 0;

			while(token[length + 1] != ' ' && token[length + 1] != '\n' && token[length + 1] != '\0') {
				length++;
			}

			char *begin, *end, temp;
			int c;
			begin = token + 1;
			end = token + 1;

			for(c = 0; c < length - 1; c++)
				end++;

			for(c = 0; c < length / 2; c++) {
				temp = *end;
				*end = *begin;
				*begin = temp;

				begin++;
				end--;
			}

			i = 0;
			}
		}
		token = strpbrk(token + 1, " \n");
	}

	return paragraph;
}

//toate thread-urile de citire ale master-ului for citi in paralel din fisier

void* threadFunction(void *var)
{
	//printf("Entered reading function\n");
	int thread_id = *(int*)var;
	char *line = NULL;
	//int found = 0;
	size_t len = 0;
	ssize_t read;

	int lines = 0;
	char *buffer = NULL;
	int bufferSize;
	int id = 0;
	int send = 1;

	/*int rank;
	MPI_Comm_rank(MPI_COMM_WORLD, &rank);*/

	FILE *f = fopen(fileName, "r");
	//printf("Thead %d opened the file %s\n", thread_id, fileName);
	if (f == NULL) {
		printf("READ FAILURE\n");
		exit(EXIT_FAILURE);
	}

	//printf("Thead %d is reading from file...\n", thread_id);
	while ((read = getline(&line, &len, f)) != -1) {
		//printf("%s", line);
        if(strcmp(line, genre[thread_id]) == 0) {
			//prepare worker for receiving message
			send = 1;
			MPI_Send(&send, 1, MPI_INT, thread_id + 1, 0, MPI_COMM_WORLD);

			id++;
			if(thread_id == 0) paragraphs++;

			//printf("Thread %d found its genre: %s; copying title in buffer...\n", thread_id, line);
			if(buffer) free(buffer);
			buffer = (char*)malloc((strlen(line) + 1) * sizeof(char));
			strcpy(buffer, line);
			while ((read = getline(&line, &len, f)) != -1 && strcmp(line, "\n") != 0) {
				//printf("Copying line in buffer...\n");
				buffer = (char*)realloc(buffer, strlen(buffer) + strlen(line) + 1);
				strcat(buffer, line);
				lines++;
			}

			bufferSize = strlen(buffer) + 1;

			//printf("Sending paragraph id...\n");
			MPI_Send(&id, 1, MPI_INT, thread_id + 1, 0, MPI_COMM_WORLD);
			//printf("Sending buffer size...\n");
			MPI_Send(&bufferSize, 1, MPI_INT, thread_id + 1, 0, MPI_COMM_WORLD);
			//printf("Sending buffer...\n");
			MPI_Send(buffer, bufferSize, MPI_CHAR, thread_id + 1, 0, MPI_COMM_WORLD);


			lines = 0;
		} else {
			if(strcmp(line, genre[0]) == 0 || strcmp(line, genre[1]) == 0 ||
			strcmp(line, genre[2]) == 0 || strcmp(line, genre[3]) == 0) {
				id++;
				if(thread_id == 0) paragraphs++;
			}
		}
    }
	//worker of thread should be done
	send = 0;
	MPI_Send(&send, 1, MPI_INT, thread_id + 1, 0, MPI_COMM_WORLD);

	//sending final number of paragraphs
	if(thread_id == 0)
		doneReading = 1;

	/*char *test = "test";
	MPI_Send(test, 4, MPI_CHAR, thread_id + 1, 0, MPI_COMM_WORLD);*/

	pthread_exit(NULL);
}

int main(int argc, char *argv[])
{
	int rank;
	int nProcesses;
	int provided;
	MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);

	MPI_Comm_rank(MPI_COMM_WORLD, &rank);
	MPI_Comm_size(MPI_COMM_WORLD, &nProcesses);
	//printf("Hello from %i/%i\n", rank, nProcesses);


	if (rank == 0) {
		int i, r;
		void *status;
		pthread_t threads[4];
		int arguments[4];
		FILE *out;

		fileName = (char*)malloc(strlen(argv[1] + 1) * sizeof(char));
		strcpy(fileName, argv[1]);

		//printf("Creating reader threads...\n");

		for(int i = 0; i < 4; i++) {
			arguments[i] = i;
			r = pthread_create(&threads[i], NULL, threadFunction, &arguments[i]);

			if (r) {
			//printf("Eroare la crearea thread-ului %d\n", i);
			exit(-1);
			}
		}


		char* pars[100000];
		int ids[100000];
		int pars_recv = 0;
		int recv_id;

		char *token = strtok(argv[1], ".");
		const char *format = ".out";
		char *outName = (char*)malloc(strlen(argv[1] + 1) * sizeof(char));
		strcpy(outName, token);
		strcat(outName, format);
		out = fopen(outName, "w");
		if(!out) {
			printf("\nNU SE POATE CREA FIȘIERUL DE IEȘIRE\n");
			exit(-1);
		}

		int buffer_size;
		char *buffer;

		while(1) {
			// recv id
			MPI_Recv(&recv_id, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			//printf("\nrecv from main id: %d\n", recv_id);

			//Recv message size
			MPI_Recv(&buffer_size, 1, MPI_INT, MPI_ANY_SOURCE, recv_id, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			//Recv message
			buffer = (char*)malloc((buffer_size + 1) * sizeof(char));
			buffer[buffer_size] = '\0';
			MPI_Recv(buffer, buffer_size, MPI_CHAR, MPI_ANY_SOURCE, recv_id, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			ids[pars_recv] = recv_id;
			pars[pars_recv] = buffer;
			pars_recv++;

			if(doneReading == 1 && pars_recv == paragraphs)
				break;
		}


		for(int print = 1; print <= paragraphs; print++) {
			for(int i = 0; i < paragraphs; i++) {
				if(ids[i] == print) {
					//print in final file
					fputs(pars[i], out);
					fflush(out);
					fputs("\n", out);
					fflush(out);

					break;
				}
			}
		}

		for (i = 0; i < 4; i++) {
		r = pthread_join(threads[i], &status);

			if (r) {
				//printf("Eroare la asteptarea thread-ului %d\n", i);
				exit(-1);
			}
		}


		free(fileName);
		fclose(out);
	} else {
		//printf("worker node\n");
		char* paragraph = NULL;
		//char buffer[255];
		int bufferSize;
		int tag;
		int recv;

		//recv intention to send or not
		MPI_Recv(&recv, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

		while(recv != 0) {
			//printf("Receiving id...\n");
			MPI_Recv(&tag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			//printf("Receiving buffer size...\n");
			MPI_Recv(&bufferSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			//printf("Receiving paragraph...\n");
			paragraph = (char*)calloc(bufferSize, sizeof(char));
			MPI_Recv(paragraph, bufferSize, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

			char* result;
			int size;
			string res;

			if(rank == 1) {
				res = horrorFunction(paragraph);
				size = res.size() + 1;
				result = &res[0];

			} else if(rank == 2) {
				result = comedyFunction(paragraph);
				size = strlen(result);
			} else if (rank == 3) {
				result = fantasyFunction(paragraph);
				size = strlen(result);
			} else if(rank == 4) {
				result = sfFunction(paragraph);
				size = strlen(result);
			}
			//send id
			//printf("\nsend id %d\n", tag);
			MPI_Send(&tag, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
			//send paragraph size
			//printf("\nsend size %d\n", tag);
			MPI_Send(&size, 1, MPI_INT, 0, tag, MPI_COMM_WORLD);
			//send buffer
			//printf("\nsend message %d\n", tag);
			MPI_Send(result, size, MPI_CHAR, 0, tag, MPI_COMM_WORLD);

			//whether to stop receiving or not
			MPI_Recv(&recv, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
			//printf("\nstop recv: %d\n", recv);
		}
	}

	MPI_Finalize();

	return 0;
}
