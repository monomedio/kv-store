# Compiler and flags
CXX = g++
CXXFLAGS = -std=c++11 -Wall -O3 -g

# Include directories
INCLUDES = -Isrc -Iexperiments

# Source files and object files for the main application
SRC_DIR = src
SOURCES = $(wildcard $(SRC_DIR)/*.cc)
OBJECTS = $(SOURCES:.cc=.o)

# Test files and object files
TEST_DIR = tests
TEST_SOURCES = $(wildcard $(TEST_DIR)/*_test.cc)
TEST_OBJECTS = $(TEST_SOURCES:.cc=.o)
MAIN_TEST = $(TEST_DIR)/main_test.cc
MAIN_TEST_OBJECT = $(MAIN_TEST:.cc=.o)
TEST_EXEC = $(TEST_DIR)/tests

# Experiment files and object files
EXP_DIR = experiments
EXP_SOURCES = $(wildcard $(EXP_DIR)/*.cc)
EXP_OBJECTS = $(EXP_SOURCES:.cc=.o)
EXP_EXEC = $(EXP_DIR)/experiments

# Default target
all: $(TEST_EXEC) $(EXP_EXEC)

# Compile and link the test executable
$(TEST_EXEC): $(OBJECTS) $(TEST_OBJECTS) $(MAIN_TEST_OBJECT)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^

# Compile and link the experiment executable
$(EXP_EXEC): $(OBJECTS) $(EXP_OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $@ $^

# Generic rule for compiling source to object files
%.o: %.cc
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

# Phony targets
.PHONY: clean test

clean:
	rm -f $(SRC_DIR)/*.o $(TEST_DIR)/*.o $(EXP_DIR)/*.o $(TEST_EXEC) $(EXP_EXEC)

test: $(TEST_EXEC)
	./$(TEST_EXEC)

# Rule to run experiments
experiment: $(EXP_EXEC)
	./$(EXP_EXEC)
