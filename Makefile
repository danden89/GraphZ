# Compiler options
CC = g++

CFLAGS=-O3 
CFLAGS+= -Wno-deprecated

CFLAGS+=  -fopenmp  -std=c++0x -L/usr/local/lib
LFLAGS= -lboost_thread -lpthread  -lconfig++ -ltcmalloc_minimal 
LFLAGS+= -ltcmalloc_minimal -laio

SRC_DIR:=./src
TEST_SRC_DIR:=./test
INC_DIR:=./inc -I./src
BIN_DIR=./bin

INCLUDES=-I$(INC_DIR) -I$(SRC_DIR)/threadpool

SOURCES+= $(wildcard inc/*.c inc/*.cpp )

TEST_SOURCES = $(wildcard $(TEST_SRC_DIR)/*.c $(TEST_SRC_DIR)/*.cpp )
TEST_SOURCES += $(wildcard inc/*.c inc/*.cpp )

sufix=app_
EXE=apps

all: $(EXE)
	@echo Finish generating $(EXE) ...

$(EXE):  app_pagerank_new app_connected_component_new app_sssp_new app_bfs_new app_randomwalk_new app_belief_propagation_new 


app_%: $(SOURCES)
	@ echo $(TEST_SOURCES)
	$(CC)  $(CFLAGS) $(INCLUDES) -o $(BIN_DIR)/$(patsubst $(sufix)%,%,$@) src/$(patsubst $(sufix)%,%,$@).cpp $(SOURCES) src/io_task.cpp $(LFLAGS)

preprocess: src/preprocess.cpp
	$(CC) -O3  -fopenmp -o preprocess src/preprocess.cpp -lstxxl -lpthread -lconfig++
	@echo Finish generating preprocess ...


clean:
	rm $(BIN_DIR)/* $(OBJS)

